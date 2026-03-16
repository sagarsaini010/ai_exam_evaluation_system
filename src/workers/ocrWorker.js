import { PubSub }     from '@google-cloud/pubsub';
import { Storage }   from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import dotenv        from 'dotenv';
import { correctOCRText }        from "./ocrCorrector.js";
import { segmentAnswersWithLLM } from "./answerSegmenter.js";

dotenv.config();

const PROJECT_ID        = process.env.GCP_PROJECT_ID      || 'secure-brook-470609-q7';
const SUBSCRIPTION      = process.env.OCR_SUBSCRIPTION    || 'exam-ocr-subscription';
const MAX_MESSAGES      = parseInt(process.env.WORKER_MAX_MESSAGES    || '5',     10);
const DOWNLOAD_TIMEOUT  = parseInt(process.env.DOWNLOAD_TIMEOUT_MS    || '20000', 10);
const OCR_LLM_TIMEOUT   = parseInt(process.env.OCR_LLM_TIMEOUT_MS     || '60000', 10);
const SEG_LLM_TIMEOUT   = parseInt(process.env.SEG_LLM_TIMEOUT_MS     || '60000', 10);

const pubsub    = new PubSub({ projectId: PROJECT_ID });
const storage   = new Storage({ projectId: PROJECT_ID });
const firestore = new Firestore({
  projectId: PROJECT_ID,
});

const log = {
  info:  (event, fields = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event, ts: new Date().toISOString(), ...fields })),
  warn:  (event, fields = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event, ts: new Date().toISOString(), ...fields })),
  error: (event, fields = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event, ts: new Date().toISOString(), ...fields })),
  debug: (event, fields = {}) => console.log  (JSON.stringify({ severity: 'DEBUG',   event, ts: new Date().toISOString(), ...fields })),
};

function safeJSONParse(str) {
  try   { return JSON.parse(str); }
  catch { return null; }
}

function withTimeout(promise, ms = 30_000, label = 'operation') {
  const timeout = new Promise((_, reject) =>
    setTimeout(
      () => reject(Object.assign(new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: 'ETIMEDOUT' })),
      ms
    )
  );
  return Promise.race([promise, timeout]);
}

function isRetryableError(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:'))  return true;
  const status = err.status ?? err.response?.status;
  if (status >= 500)   return true;
  if (status === 429)  return true;
  return false;
}

function docId(ocrPath) {
  return Buffer.from(ocrPath).toString('base64url');
}

async function markProcessedAndAck(ocrPath, message, extraFields = {}) {
  await firestore
    .collection('ocr_processed')
    .doc(docId(ocrPath))
    .set(
      { ocrPath, processedAt: new Date().toISOString(), ...extraFields },
      { merge: true }
    );
  message.ack();
}

/* ─── Layout → plain text converter ──────────────────────────────────────────
   LLM ko layout-aware text deta hai:
   "Page 1 (confidence: 0.81):\nline1\nline2\n\nPage 2..."
   Isse LLM ko page structure pata rehta hai, plain blob nahi milta.
─────────────────────────────────────────────────────────────────────────── */
function layoutToLLMText(pages) {
  if (!pages || pages.length === 0) return null;

  return pages
    .map((p) => {
      const header = `--- Page ${p.page} (confidence: ${p.confidence ?? 'N/A'}) ---`;
      const body   = (p.lines || []).join('\n');
      return `${header}\n${body}`;
    })
    .join('\n\n');
}

let inFlight    = 0;
let shuttingDown = false;

function beginWork() {
  if (shuttingDown) return false;
  inFlight++;
  return true;
}

function endWork() {
  inFlight--;
}

function waitForInFlight(pollMs = 100) {
  return new Promise((resolve) => {
    const check = () => (inFlight === 0 ? resolve() : setTimeout(check, pollMs));
    check();
  });
}

async function handleMessage(message) {
  const messageId  = message.id;
  const rawPayload = message.data.toString();
  const startedAt  = Date.now();

  log.info('WORKER_MESSAGE_RECEIVED', { messageId });

  const data = safeJSONParse(rawPayload);

  if (!data?.bucket || !data?.ocrPath) {
    log.error('WORKER_INVALID_PAYLOAD', { messageId, rawPayload: rawPayload.slice(0, 500) });
    message.ack();
    return;
  }

  const { bucket, ocrPath } = data;

  log.info('WORKER_PAYLOAD_PARSED', { messageId, bucket, ocrPath });

  const processedDocRef = firestore.collection('ocr_processed').doc(docId(ocrPath));

  log.info('CHECKING_IDEMPOTENCY', {
    messageId,
    ocrPath,
    docId:    docId(ocrPath),
    fullPath: `ocr_processed/${docId(ocrPath)}`,
  });

  let snap;
  try {
    snap = await withTimeout(
      processedDocRef.get(),
      10_000,
      'Firestore idempotency check'
    );
  } catch (err) {
    log.error('FIRESTORE_IDEMPOTENCY_CHECK_FAILED', {
      messageId,
      ocrPath,
      error:   err.message,
      code:    err.code,
      details: err.details,
    });
    throw err;
  }

  if (snap.exists) {
    log.warn('WORKER_DUPLICATE_MESSAGE', { messageId, ocrPath });
    message.ack();
    return;
  }

  log.info('GCS_DOWNLOAD_START', { messageId, bucket, ocrPath });

  let fileBuffer;
  try {
    [fileBuffer] = await withTimeout(
      storage.bucket(bucket).file(ocrPath).download(),
      DOWNLOAD_TIMEOUT,
      `GCS download ${ocrPath}`
    );
  } catch (err) {
    log.error('GCS_DOWNLOAD_FAILED', {
      messageId,
      bucket,
      ocrPath,
      error: err.message,
      code:  err.code,
    });
    throw err;
  }

  log.info('GCS_DOWNLOAD_COMPLETE', { messageId, ocrPath, bytes: fileBuffer.length });

  const ocrJson = safeJSONParse(fileBuffer.toString());

  const student = data.student ?? ocrJson?.student ?? null;

  if (!ocrJson?.text) {
    log.error('WORKER_INVALID_OCR_JSON', { messageId, ocrPath });
    await markProcessedAndAck(ocrPath, message, { skippedReason: 'invalid_ocr_json' });
    return;
  }

  const rawText = ocrJson.text;

  if (!rawText?.trim()) {
    log.error('WORKER_EMPTY_OCR_TEXT', { messageId, ocrPath });
    await markProcessedAndAck(ocrPath, message, { skippedReason: 'empty_text' });
    return;
  }

  const cleanedText = rawText.replace(/\s+/g, ' ').trim();

  // ── Layout pages ──────────────────────────────────────────────────────────
  const layoutPages = ocrJson.pages || [];

  // ── Layout print karo — GCP Logs mein dikh jayega ────────────────────────
  log.info('WORKER_LAYOUT_PAGES', {
    messageId,
    ocrPath,
    totalLayoutPages: layoutPages.length,
    pages: layoutPages.map(p => ({
      page:       p.page,
      confidence: p.confidence,
      lineCount:  p.lines?.length ?? 0,
      firstLine:  p.lines?.[0]                        ?? null,
      lastLine:   p.lines?.[p.lines.length - 1]       ?? null,
    })),
  });

  log.info('WORKER_TEXT_READY', {
    messageId,
    ocrPath,
    textLength:     cleanedText.length,
    totalPages:     ocrJson.totalPages,
    avgConfidence:  ocrJson.avgConfidence  ?? null,
    pageConfidence: ocrJson.pageConfidence ?? null,
    student,
  });

  // ── Layout text banao LLM ke liye ─────────────────────────────────────────
  // Agar layout pages available hain toh layout-aware text use karo,
  // warna fallback cleanedText pe
  const llmInputText = layoutPages.length > 0
    ? layoutToLLMText(layoutPages)
    : cleanedText;

  log.info('WORKER_LLM_INPUT_READY', {
    messageId,
    ocrPath,
    source:     layoutPages.length > 0 ? 'layout_pages' : 'cleaned_text',
    inputLength: llmInputText.length,
  });

  // ── LLM OCR correction ────────────────────────────────────────────────────
  let correctedText = llmInputText;

  try {
    log.info('OCR_CORRECTION_START', { messageId, ocrPath, textLength: llmInputText.length });

    correctedText = await withTimeout(
      correctOCRText(llmInputText),
      OCR_LLM_TIMEOUT,
      'OCR correction'
    );

    log.info('OCR_CORRECTION_COMPLETE', {
      messageId,
      ocrPath,
      inputLength:  llmInputText.length,
      outputLength: correctedText.length,
    });

  } catch (err) {
    log.error('OCR_CORRECTION_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      code:  err.code,
      note:  'Falling back to llmInputText',
    });
  }

  // ── LLM answer segmentation ───────────────────────────────────────────────
  let segmentedAnswers = { questions: [] };

  try {
    log.info('ANSWER_SEGMENTATION_START', { messageId, ocrPath, textLength: correctedText.length });

    segmentedAnswers = await withTimeout(
      segmentAnswersWithLLM(correctedText),
      SEG_LLM_TIMEOUT,
      'answer segmentation'
    );

    log.info('ANSWER_SEGMENTATION_COMPLETE', {
      messageId,
      ocrPath,
      questionsFound: segmentedAnswers?.questions?.length ?? 0,
    });

    log.debug('SEGMENTED_ANSWERS_DETAIL', {
      messageId,
      ocrPath,
      segmentedAnswers,
    });

  } catch (err) {
    log.error('ANSWER_SEGMENTATION_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      code:  err.code,
      note:  'Falling back to empty questions array',
    });
  }

  // ── Persist results to Firestore ──────────────────────────────────────────
  log.info('FIRESTORE_WRITE_START', { messageId, ocrPath });

  try {
    await withTimeout(
      firestore.collection('exam_answers').doc(docId(ocrPath)).set({
        ocrPath,
        bucket,
        student:          student ?? null,
        rawText,
        cleanedText,
        llmInputText,                              // layout-aware text jo LLM ko diya
        correctedText,
        segmentedAnswers,
        totalPages:       ocrJson.totalPages    ?? null,
        avgConfidence:    ocrJson.avgConfidence  ?? null,
        pageConfidence:   ocrJson.pageConfidence ?? null,
        pages:            layoutPages,             // full layout pages Firestore mein
        processedAt:      new Date().toISOString(),
        processingTimeMs: Date.now() - startedAt,
      }),
      15_000,
      'Firestore exam_answers write'
    );
  } catch (err) {
    log.error('FIRESTORE_WRITE_FAILED', {
      messageId,
      ocrPath,
      error:   err.message,
      code:    err.code,
      details: err.details,
    });
    throw err;
  }

  log.info('FIRESTORE_WRITE_COMPLETE', { messageId, ocrPath });

  try {
    await markProcessedAndAck(ocrPath, message, {
      student:          student ?? null,
      questionsFound:   segmentedAnswers?.questions?.length ?? 0,
      processingTimeMs: Date.now() - startedAt,
    });
  } catch (err) {
    log.error('MARK_PROCESSED_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      note:  'Results saved to exam_answers but idempotency record may be missing',
    });
    throw err;
  }

  log.info('WORKER_MESSAGE_PROCESSED', {
    messageId,
    ocrPath,
    student,
    questionsFound:   segmentedAnswers?.questions?.length ?? 0,
    processingTimeMs: Date.now() - startedAt,
  });
}

const subscription = pubsub.subscription(SUBSCRIPTION, {
  flowControl: { maxMessages: MAX_MESSAGES },
});

log.info('WORKER_STARTED', {
  subscription:  SUBSCRIPTION,
  maxMessages:   MAX_MESSAGES,
  projectId:     PROJECT_ID,
  downloadTimeout: DOWNLOAD_TIMEOUT,
  ocrLlmTimeout:   OCR_LLM_TIMEOUT,
  segLlmTimeout:   SEG_LLM_TIMEOUT,
});

subscription.on('message', async (message) => {
  if (!beginWork()) {
    log.warn('WORKER_SHUTTING_DOWN_NACK', { messageId: message.id });
    message.nack();
    return;
  }

  try {
    await handleMessage(message);
  } catch (err) {
    const messageId = message.id;

    if (isRetryableError(err)) {
      log.warn('WORKER_RETRYABLE_ERROR', {
        messageId,
        error: err.message,
        code:  err.code,
        note:  'NACKing — Pub/Sub will redeliver',
      });
      message.nack();
    } else {
      log.error('WORKER_PERMANENT_ERROR', {
        messageId,
        error: err.message,
        code:  err.code,
        note:  'ACKing — will not redeliver to avoid infinite loop',
      });
      message.ack();
    }
  } finally {
    endWork();
  }
});

subscription.on('error', (err) => {
  log.error('WORKER_SUBSCRIPTION_ERROR', {
    error:   err.message,
    code:    err.code,
    details: err.details,
  });
});

subscription.on('close', () => {
  log.info('WORKER_SUBSCRIPTION_CLOSED', {});
});

async function shutdown(signal) {
  log.info('WORKER_SHUTDOWN_START', { signal, inFlight });
  shuttingDown = true;

  try {
    await subscription.close();
    log.info('WORKER_SUBSCRIPTION_STOPPED', { signal });
  } catch (err) {
    log.error('WORKER_SUBSCRIPTION_CLOSE_FAILED', { signal, error: err.message });
  }

  log.info('WORKER_WAITING_FOR_IN_FLIGHT', { signal, inFlight });
  await waitForInFlight();

  log.info('WORKER_SHUTDOWN_COMPLETE', { signal });
  process.exit(0);
}

process.on('unhandledRejection', (reason, promise) => {
  log.error('UNHANDLED_REJECTION', {
    reason: reason?.message ?? String(reason),
    stack:  reason?.stack,
  });
});

process.on('uncaughtException', (err) => {
  log.error('UNCAUGHT_EXCEPTION', {
    error: err.message,
    stack: err.stack,
  });
  process.exit(1);
});

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));