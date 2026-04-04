import { GoogleGenerativeAI } from '@google/generative-ai';
import dotenv from "dotenv";

dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const embeddingModel = genAI.getGenerativeModel({apiKey: process.env.GEMINI_API_KEY, model: process.env.GEMINI_EMBEDDING_MODEL});

const RETRY_CONFIG = { maxAttempts: 3, baseDelayMs: 2000, maxDelayMs: 15000 };

function withTimeout(promise, ms, label = 'operation') {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(Object.assign(
        new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: 'ETIMEDOUT' }
      )), ms)
    ),
  ]);
}

function isRetryable(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:')) return true;
  const status = err.status ?? err.response?.status;
  if (status === 429 || status >= 500) return true;
  return false;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

const log = {
  info:  (e, f = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event: e, ts: new Date().toISOString(), ...f })),
  warn:  (e, f = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event: e, ts: new Date().toISOString(), ...f })),
  error: (e, f = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event: e, ts: new Date().toISOString(), ...f })),
};

// ─── Single embedding ─────────────────────────────────────────────────────────
export async function generateEmbedding(text, taskType = 'RETRIEVAL_DOCUMENT') {
  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = Math.min(
          RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 500,
          RETRY_CONFIG.maxDelayMs
        );
        log.warn('EMBEDDING_RETRY', { attempt, delay: Math.round(delay), error: lastError?.message });
        await sleep(delay);
      }

      const result = await withTimeout(
        embeddingModel.embedContent({
          content: { parts: [{ text }] },
          taskType: 'RETRIEVAL_DOCUMENT',
        }),
        60_000,
        'embedding predict'
      );

      const embedding = result.embedding.values;

      log.info('EMBEDDING_SUCCESS', { dimensions: embedding.length });
      return embedding;

    } catch (err) {
      lastError = err;
      if (!isRetryable(err)) {
        log.error('EMBEDDING_NON_RETRYABLE', { attempt: attempt + 1, error: err.message, code: err.code });
        break;
      }
      log.warn('EMBEDDING_RETRYABLE_ERROR', { attempt: attempt + 1, error: err.message });
    }
  }

  throw Object.assign(
    new Error(lastError?.message ?? 'Embedding failed'),
    { code: lastError?.code ?? 'EMBEDDING_FAILED' }
  );
}

// ─── Batch embeddings — production ready ──────────────────────────────────────
// Ek saath sab nahi — batchSize ke groups mein, beech mein delay
// Default: 10 ek saath, 500ms gap — 1500/min quota ke andar rehta hai
export async function generateEmbeddingsBatch(texts, batchSize = 10, delayMs = 500) {
  const results = [];
  let failed = 0;

  for (let i = 0; i < texts.length; i += batchSize) {
    const batch      = texts.slice(i, i + batchSize);
    const batchIndex = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(texts.length / batchSize);

    log.info('EMBEDDING_BATCH_START', {
      batchIndex,
      totalBatches,
      batchSize: batch.length,
      totalTexts: texts.length,
    });

    const batchResults = await Promise.allSettled(
      batch.map(text => generateEmbedding(text))
    );

    for (const result of batchResults) {
      if (result.status === 'fulfilled') {
        results.push({ success: true, embedding: result.value });
      } else {
        failed++;
        results.push({ success: false, error: result.reason?.message });
        log.error('EMBEDDING_BATCH_ITEM_FAILED', { error: result.reason?.message });
      }
    }

    log.info('EMBEDDING_BATCH_COMPLETE', { batchIndex, totalBatches, failed });

    // Last batch ke baad delay mat lagao
    if (i + batchSize < texts.length) {
      await sleep(delayMs);
    }
  }

  log.info('EMBEDDING_ALL_COMPLETE', {
    total:     texts.length,
    succeeded: texts.length - failed,
    failed,
  });

  return results;
}