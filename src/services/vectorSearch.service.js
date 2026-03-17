import { IndexServiceClient } from '@google-cloud/aiplatform/build/src/v1/index.js';

const PROJECT_ID    = process.env.GCP_PROJECT_ID       || 'secure-brook-470609-q7';
const LOCATION      = process.env.GCP_LOCATION         || 'asia-south1';
const INDEX_ID      = process.env.VECTOR_INDEX_ID;
const INDEX_NAME    = `projects/${PROJECT_ID}/locations/${LOCATION}/indexes/${INDEX_ID}`;

const client = new IndexServiceClient({
  apiEndpoint: `${LOCATION}-aiplatform.googleapis.com`,
});

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
  if (err.code === 8) return true;
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

export async function upsertDatapoint(datapointId, embedding, examId, subjectId) {
  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = Math.min(
          RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 500,
          RETRY_CONFIG.maxDelayMs
        );
        log.warn('VECTOR_UPSERT_RETRY', { datapointId, attempt, error: lastError?.message });
        await sleep(delay);
      }

      await withTimeout(
        client.upsertDatapoints({
          index:      INDEX_NAME,
          datapoints: [{
            datapointId,
            featureVector: embedding,
            restricts: [
              { namespace: 'examId',    allowList: [examId]    },
              { namespace: 'subjectId', allowList: [subjectId] },
            ],
          }],
        }),
        30_000,
        `upsertDatapoint ${datapointId}`
      );

      log.info('VECTOR_UPSERT_SUCCESS', { datapointId });
      return;

    } catch (err) {
      lastError = err;
      if (!isRetryable(err)) {
        log.error('VECTOR_UPSERT_NON_RETRYABLE', { datapointId, attempt: attempt + 1, error: err.message });
        break;
      }
      log.warn('VECTOR_UPSERT_RETRYABLE_ERROR', { datapointId, attempt: attempt + 1, error: err.message });
    }
  }

  throw Object.assign(
    new Error(lastError?.message ?? 'Vector upsert failed'),
    { code: lastError?.code ?? 'VECTOR_UPSERT_FAILED' }
  );
}