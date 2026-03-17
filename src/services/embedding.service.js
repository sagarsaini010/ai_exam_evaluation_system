import { PredictionServiceClient } from '@google-cloud/aiplatform';

const PROJECT_ID = process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7';
const LOCATION   = 'asia-south1';
const MODEL      = 'text-multilingual-embedding-002';
const ENDPOINT   = `projects/${PROJECT_ID}/locations/${LOCATION}/publishers/google/models/${MODEL}`;

const client = new PredictionServiceClient({
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

export async function generateEmbedding(text) {
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

      const [response] = await withTimeout(
        client.predict({
          endpoint:  ENDPOINT,
          instances: [{
            structValue: {
              fields: {
                content:   { stringValue: text },
                task_type: { stringValue: 'RETRIEVAL_DOCUMENT' },
              },
            },
          }],
          parameters: {
            structValue: {
              fields: {
                outputDimensionality: { numberValue: 768 },
              },
            },
          },
        }),
        60_000,
        'embedding predict'
      );

      const embedding = response.predictions[0].structValue
        .fields.embeddings.structValue
        .fields.values.listValue.values
        .map(v => v.numberValue);

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