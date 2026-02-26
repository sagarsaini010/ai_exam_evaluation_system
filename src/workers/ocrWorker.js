import { PubSub } from '@google-cloud/pubsub';
import { Storage } from '@google-cloud/storage';
import dotenv from 'dotenv';

dotenv.config();

/* ==============================
   CONFIG
============================== */

const PROJECT_ID = 'secure-brook-470609-q7';
const subscriptionName = 'exam-ocr-subscription';

const pubsub = new PubSub({
  projectId: PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

const storage = new Storage({
  projectId: PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

/* ==============================
   SAFETY HELPERS
============================== */

function isRetryableError(err) {
  // Network / temporary API issues
  if (!err) return false;

  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) {
    return true;
  }

  if (err.response?.status >= 500) return true; // server errors
  if (err.response?.status === 429) return true; // rate limit

  return false;
}

function safeJSONParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

// Timeout wrapper to prevent hanging
function withTimeout(promise, ms = 30000) {
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Processing timeout')), ms)
  );
  return Promise.race([promise, timeout]);
}

/* ==============================
   IDEMPOTENCY CHECK (IMPORTANT)
   Replace with DB check in real app
============================== */

async function alreadyProcessed(ocrPath) {
  // TODO: replace with DB lookup
  return false;
}

/* ==============================
   MAIN WORKER
============================== */

const subscription = pubsub.subscription(subscriptionName, {
  flowControl: {
    maxMessages: 5, // prevent overload
  },
});

console.log('🚀 OCR Worker started and listening...');

subscription.on('message', async (message) => {
  const messageId = message.id;
  const rawPayload = message.data.toString();

  console.log(`📩 Message received: ${messageId}`);

  try {
    /* ---- Safe JSON Parse ---- */
    const data = safeJSONParse(rawPayload);

    if (!data || !data.bucket || !data.ocrPath) {
      console.error('❌ Invalid message payload:', rawPayload);
      message.ack(); // permanent bad message
      return;
    }

    /* ---- Idempotency Guard ---- */
    if (await alreadyProcessed(data.ocrPath)) {
      console.log('⚠️ Already processed:', data.ocrPath);
      message.ack();
      return;
    }

    /* ---- Download OCR JSON ---- */
    const [fileBuffer] = await withTimeout(
      storage.bucket(data.bucket).file(data.ocrPath).download(),
      20000
    );

    const ocrJson = safeJSONParse(fileBuffer.toString());

    if (!ocrJson || !ocrJson.text) {
      throw new Error('Invalid OCR JSON structure');
    }

    /* ---- Clean Text ---- */
    const cleanedText = ocrJson.text
      .replace(/\s+/g, ' ')
      .trim();

    if (!cleanedText.length) {
      throw new Error('Empty OCR text');
    }

    /* ---- TODO: Segmentation + LLM Grading ---- */
    // IMPORTANT: Wrap LLM call in withTimeout and retry logic
    console.log(`✅ OCR text cleaned for ${data.ocrPath}, Text: ${cleanedText}`);
    console.log(`✅ Processed successfully: ${data.ocrPath}`);

    message.ack();
  } catch (err) {
    console.error(`❌ Error processing message ${messageId}:`, err.message);

    if (isRetryableError(err)) {
      console.log('🔁 Retryable error — NACK');
      message.nack();
    } else {
      console.log('🚫 Permanent error — ACK to avoid loop');
      message.ack();
    }
  }
});

/* ==============================
   GRACEFUL SHUTDOWN
============================== */

process.on('SIGINT', async () => {
  console.log('🛑 Shutting down worker...');
  await subscription.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM received...');
  await subscription.close();
  process.exit(0);
});


