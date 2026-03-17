import { VertexAI } from '@google-cloud/vertexai';

const PROJECT_ID = process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7';
const LOCATION   = 'asia-south1';

const vertexAI = new VertexAI({ project: PROJECT_ID, location: LOCATION });

const model = vertexAI.getGenerativeModel({
  model:            'gemini-3-flash-preview',
  generationConfig: { temperature: 0 },
});

/* ─── Retry config ────────────────────────────────────────────────────────── */
const RETRY_CONFIG = {
  maxAttempts: 3,
  baseDelayMs: 1000,
  maxDelayMs:  10000,
};

/* ─── Timeout protection ─────────────────────────────────────────────────── */
function withTimeout(promise, ms, label = 'operation') {
  const timeout = new Promise((_, reject) =>
    setTimeout(
      () => reject(Object.assign(new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: 'ETIMEDOUT' })),
      ms
    )
  );
  return Promise.race([promise, timeout]);
}

/* ─── Retryable error detection ──────────────────────────────────────────── */
function isRetryable(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:')) return true;
  const status = err.status ?? err.response?.status;
  if (status === 429) return true;
  if (status >= 500)  return true;
  return false;
}

/* ─── Exponential backoff ────────────────────────────────────────────────── */
function getBackoffDelay(attempt) {
  const jitter = Math.random() * 500;
  return Math.min(
    RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt) + jitter,
    RETRY_CONFIG.maxDelayMs
  );
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/* ─── Core LLM call — Vertex AI SDK ──────────────────────────────────────── */
async function callLLM(ocrText) {
  const prompt = `
You are cleaning OCR text from a handwritten exam sheet.

Your task is ONLY to fix obvious OCR noise.

Allowed fixes:
- join broken words
- remove random OCR characters
- fix spacing
- fix math symbols (π, √, sin, cos etc)
- fix obvious spelling errors

STRICT RULES:

1. DO NOT change numbers.
2. DO NOT change mathematical expressions.
3. DO NOT guess missing characters.
4. If uncertain, keep the original text.

Preserve all Hindi text exactly.

Return ONLY cleaned text.

OCR TEXT:
${ocrText}
`;

  // Vertex AI SDK response shape
  const result = await model.generateContent({
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
  });

  const raw = result.response.candidates?.[0]?.content?.parts?.[0]?.text ?? '';

  if (!raw.trim()) {
    throw Object.assign(new Error('LLM returned empty response'), { nonRetryable: true });
  }

  return raw;
}

/* ─── Main export — logic unchanged, only SDK call migrated ──────────────── */
export async function correctOCRText(ocrText) {
  console.log('\n=========== OCR CORRECTION STARTED ===========');
  console.log(`timestamp  : ${new Date().toISOString()}`);
  console.log(`inputLength: ${ocrText?.length ?? 0} chars`);
  console.log('===============================================\n');

  console.log('\n=========== OCR BEFORE LLM CORRECTION ===========\n');
  console.log(ocrText);
  console.log('\n==================================================\n');

  if (!ocrText?.trim()) {
    console.warn('correctOCRText: empty text provided, skipping correction');
    return ocrText ?? '';
  }

  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = getBackoffDelay(attempt - 1);
        console.warn(`correctOCRText: retry ${attempt}/${RETRY_CONFIG.maxAttempts - 1} after ${Math.round(delay)}ms — ${lastError?.message}`);
        await sleep(delay);
      }

      console.log(`correctOCRText: calling Gemini (attempt ${attempt + 1})...`);

      const correctedText = await withTimeout(
        callLLM(ocrText),
        30_000,
        'OCR correction'
      );

      console.log('\n=========== OCR AFTER LLM CORRECTION ===========\n');
      console.log(correctedText);
      console.log('\n=================================================\n');

      console.log('\n=========== OCR CORRECTION COMPLETE ===========');
      console.log(`outputLength: ${correctedText.trim().length} chars`);
      console.log('================================================\n');

      return correctedText.trim();

    } catch (err) {
      lastError = err;

      if (err.nonRetryable || !isRetryable(err)) {
        console.error(`correctOCRText: non-retryable on attempt ${attempt + 1}:`, err.message);
        break;
      }

      console.warn(`correctOCRText: retryable on attempt ${attempt + 1}:`, err.message, err.status ? `(HTTP ${err.status})` : '');
    }
  }

  console.error('correctOCRText: all attempts failed, returning original. Last error:', lastError?.message);
  return ocrText.trim();
}