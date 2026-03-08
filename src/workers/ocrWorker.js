import { PubSub }     from '@google-cloud/pubsub';
import { Storage }   from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import dotenv        from 'dotenv';
import { correctOCRText } from "./ocrCorrector.js";
import { segmentAnswersWithLLM } from "./answerSegmenter.js";
dotenv.config();

/* ─── Config ─────────────────────────────────────────────────────────────────
   All values from env — no hardcoded project IDs or key files.
   ADC is used automatically on GCP (Cloud Run, GKE, GCF).
   Locally: run `gcloud auth application-default login`
──────────────────────────────────────────────────────────────────────────── */
const PROJECT_ID       = process.env.GCP_PROJECT_ID  || 'secure-brook-470609-q7';
const SUBSCRIPTION     = process.env.OCR_SUBSCRIPTION || 'exam-ocr-subscription';
const MAX_MESSAGES     = parseInt(process.env.WORKER_MAX_MESSAGES || '5', 10);
const DOWNLOAD_TIMEOUT = 20_000;  // ms

const pubsub    = new PubSub({ projectId: PROJECT_ID });
const storage   = new Storage({ projectId: PROJECT_ID });
const firestore = new Firestore({ projectId: PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
 });

/* ─── Structured logger ──────────────────────────────────────────────────────
   Mirrors the logger in ocr.function.js — every line is a JSON object so
   Cloud Logging can filter/alert on event names and fields directly.
──────────────────────────────────────────────────────────────────────────── */
const log = {
  info:  (event, fields = {}) => console.log(JSON.stringify({ severity: 'INFO',    event, ...fields })),
  warn:  (event, fields = {}) => console.warn(JSON.stringify({ severity: 'WARNING', event, ...fields })),
  error: (event, fields = {}) => console.error(JSON.stringify({ severity: 'ERROR',  event, ...fields })),
};

/* ─── Helpers ─────────────────────────────────────────────────────────────── */

function safeJSONParse(str) {
  try   { return JSON.parse(str); }
  catch { return null; }
}

/** Races a promise against a timeout. Throws on expiry. */
function withTimeout(promise, ms = 30_000, label = 'operation') {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error(`Timeout: ${label} exceeded ${ms}ms`)), ms)
  );
  return Promise.race([promise, timeout]);
}

/**
 * Returns true for transient errors that are safe to NACK and retry.
 * Non-retryable errors (bad data, empty text) should be ACK'd to avoid
 * an infinite Pub/Sub redelivery loop.
 */
function isRetryableError(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:'))    return true;  // withTimeout() errors
  if (err.response?.status >= 500)            return true;  // upstream server errors
  if (err.response?.status === 429)           return true;  // rate limited
  return false;
}

/* ─── Idempotency ─────────────────────────────────────────────────────────── */

/** Stable Firestore doc ID derived from the GCS path. */
function docId(ocrPath) {
  return Buffer.from(ocrPath).toString('base64url');
}

async function alreadyProcessed(ocrPath) {
  const docRef = firestore.collection('ocr_processed').doc(docId(ocrPath));

  log.info('CHECKING_IDEMPOTENCY', {
    ocrPath,
    docId: docId(ocrPath),
    fullPath: `ocr_processed/${docId(ocrPath)}`
  });

  const snap = await docRef.get();
  return snap.exists;
}

/**
 * Marks the path as processed AND acks the Pub/Sub message in the right order:
 *
 *   1. Write to Firestore first (durable)
 *   2. Ack the message second
 *
 * If the process crashes between 1 and 2, Pub/Sub redelivers but the
 * idempotency check at the top of the handler catches it immediately.
 * The reverse order (ack → write) risks silently losing the message if
 * the process dies before the Firestore write completes.
 */
async function markProcessedAndAck(ocrPath, message, extraFields = {}) {
  await firestore
    .collection('ocr_processed')
    .doc(docId(ocrPath))
    .set({ ocrPath, processedAt: new Date().toISOString(), ...extraFields }, { merge: true });

  message.ack();
}

/* ─── Shutdown coordination ───────────────────────────────────────────────── */

let inFlight = 0;
let shuttingDown = false;

/** Call at the start of each message handler. Returns false if shutting down. */
function beginWork() {
  if (shuttingDown) return false;
  inFlight++;
  return true;
}

/** Call when a message handler finishes (success or failure). */
function endWork() {
  inFlight--;
}

/**
 * Waits for all in-flight handlers to finish before resolving.
 * Prevents messages being abandoned mid-processing during shutdown.
 */
function waitForInFlight(pollMs = 100) {
  return new Promise((resolve) => {
    const check = () => (inFlight === 0 ? resolve() : setTimeout(check, pollMs));
    check();
  });
}

/* ─── Subscription ────────────────────────────────────────────────────────── */

const subscription = pubsub.subscription(SUBSCRIPTION, {
  flowControl: { maxMessages: MAX_MESSAGES },
});

log.info('WORKER_STARTED', { subscription: SUBSCRIPTION, maxMessages: MAX_MESSAGES });

subscription.on('message', async (message) => {
  if (!beginWork()) {
    // Worker is shutting down — nack so another instance picks it up
    message.nack();
    return;
  }

  const messageId  = message.id;
  const rawPayload = message.data.toString();

  log.info('WORKER_MESSAGE_RECEIVED', { messageId });

  try {
    /* ── Validate payload ──────────────────────────────────────────────── */
    const data = safeJSONParse(rawPayload);

    if (!data?.bucket || !data?.ocrPath) {
      log.error('WORKER_INVALID_PAYLOAD', { messageId, rawPayload });
      message.ack();  // permanent bad message — ack to stop redelivery
      return;
    }

    const { bucket, ocrPath, student } = data;

     /* ── Idempotency guard ─────────────────────────────────────────────── */
    const docRef = firestore.collection('ocr_processed').doc(docId(ocrPath));

    log.info('CHECKING_IDEMPOTENCY', {
      messageId,
      ocrPath,
      docId: docId(ocrPath),
      fullPath: `ocr_processed/${docId(ocrPath)}`
    });

    let snap;
    try {
      snap = await docRef.get();
    } catch (err) {
      log.error('FIRESTORE_PERMISSION_CHECK_FAILED', {
        messageId,
        ocrPath,
        error: err.message,
        code: err.code,
        details: err.details
      });
      throw err;  // yeh upar wale catch block mein jayega
    }

    if (snap.exists) {
      log.warn('WORKER_DUPLICATE_MESSAGE', { messageId, ocrPath });
      message.ack();
      return;
    }

    /* ── Download OCR JSON ─────────────────────────────────────────────── */
    const [fileBuffer] = await withTimeout(
      storage.bucket(bucket).file(ocrPath).download(),
      DOWNLOAD_TIMEOUT,
      `GCS download ${ocrPath}`
    );

    const ocrJson = safeJSONParse(fileBuffer.toString());

    if (!ocrJson?.text) {
      // Bad OCR output — not retryable, mark done to avoid reprocessing
      log.error('WORKER_INVALID_OCR_JSON', { messageId, ocrPath });
      await markProcessedAndAck(ocrPath, message, { skippedReason: 'invalid_ocr_json' });
      return;
    }

  /* ── Clean + LLM OCR correction ────────────────────────────────────── */

const rawText = ocrJson.text;

if (!rawText || !rawText.trim()) {
  log.error('WORKER_EMPTY_OCR_TEXT', { messageId, ocrPath });
  await markProcessedAndAck(ocrPath, message, { skippedReason: 'empty_text' });
  return;
}

/* basic whitespace cleaning */
const cleanedText = rawText.replace(/\s+/g, ' ').trim();

log.info('WORKER_TEXT_READY', {
  messageId,
  ocrPath,
  textLength: cleanedText.length,
  totalPages: ocrJson.totalPages,
  student,
});

/* LLM OCR correction */

let correctedText;
let segmentedAnswers = { questions: [] };
try {

  correctedText = await withTimeout(
    correctOCRText(cleanedText),
    60000,
    "OCR correction"
  );


try {

  segmentedAnswers = await withTimeout(
    segmentAnswersWithLLM(correctedText),
    60000,
    "answer segmentation"
  );

  console.log("\n========== SEGMENTED ANSWERS ==========\n");
  console.log(JSON.stringify(segmentedAnswers, null, 2));
  console.log("\n=======================================\n");

} catch (err) {

  log.error("ANSWER_SEGMENTATION_FAILED", {
    messageId,
    error: err.message
  });

  segmentedAnswers = { questions: [] };

}

} catch (err) {

  log.error("OCR_CORRECTION_FAILED", {
    messageId,
    ocrPath,
    error: err.message
  });

  correctedText = cleanedText;
}

    /* ── Print extracted & cleaned text ────────────────────────────────── */
    // console.log('\n========== EXTRACTED & CLEANED TEXT ==========');
    // console.log(`Student  : ${student ?? 'N/A'}`);
    // console.log(`Pages    : ${ocrJson.totalPages ?? 'N/A'}`);
    // console.log(`Length   : ${cleanedText.length} characters`);
    // console.log('----------------------------------------------');
    // console.log(correctedText);
    // console.log('==============================================\n');

    /* ── TODO: Segmentation + LLM grading ─────────────────────────────────
       When you add LLM grading here, wrap the call in withTimeout() and
       use isRetryableError() to decide whether to nack (transient failure)
       or ack (bad input that will never succeed).

       Example structure:
         let gradingResult;
         try {
           gradingResult = await withTimeout(callLLM(cleanedText, student), 60_000, 'LLM grading');
         } catch (err) {
           if (isRetryableError(err)) { message.nack(); return; }
           await markProcessedAndAck(ocrPath, message, { skippedReason: 'llm_failed' });
           return;
         }
    ────────────────────────────────────────────────────────────────────── */

    /* ── Mark done and ack ─────────────────────────────────────────────── */
  await firestore.collection("exam_answers")
.doc(docId(ocrPath))
.set({
  ocrPath,
  correctedText,
  segmentedAnswers,
  student,
  processedAt: new Date().toISOString()
});

await markProcessedAndAck(ocrPath, message, { student });

    log.info('WORKER_MESSAGE_PROCESSED', { messageId, ocrPath });

  } catch (err) {
    if (isRetryableError(err)) {
      log.warn('WORKER_RETRYABLE_ERROR', { messageId, error: err.message, code: err.code });
      message.nack();
    } else {
      log.error('WORKER_PERMANENT_ERROR', { messageId, error: err.message });
      message.ack();  // ack to avoid infinite redelivery loop
    }
  } finally {
    endWork();
  }
});

subscription.on('error', (err) => {
  log.error('WORKER_SUBSCRIPTION_ERROR', { error: err.message, code: err.code });
});

/* ─── Graceful shutdown ───────────────────────────────────────────────────── */

async function shutdown(signal) {
  log.info('WORKER_SHUTDOWN_START', { signal });
  shuttingDown = true;

  // Stop accepting new messages
  await subscription.close();

  // Wait for all in-flight handlers to complete
  await waitForInFlight();

  log.info('WORKER_SHUTDOWN_COMPLETE', { signal });
  process.exit(0);
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));