import { DocumentProcessorServiceClient } from '@google-cloud/documentai/build/src/v1/index.js';

const PROJECT_ID     = process.env.GCP_PROJECT_ID;
const LOCATION       = process.env.DOCUMENT_AI_LOCATION;
const PROCESSOR_ID   = process.env.DOCUMENT_AI_PROCESSOR_ID;
const PROCESSOR_NAME = `projects/${PROJECT_ID}/locations/${LOCATION}/processors/${PROCESSOR_ID}`;

// Typed PDFs are small — 60s is very generous, prevents forever-hang
const DOC_AI_TIMEOUT_MS = 60_000;

const client = new DocumentProcessorServiceClient({
  apiEndpoint: `${LOCATION}-documentai.googleapis.com`,
});

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

const log = {
  info:  (e, f = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event: e, ts: new Date().toISOString(), ...f })),
  warn:  (e, f = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event: e, ts: new Date().toISOString(), ...f })),
  error: (e, f = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event: e, ts: new Date().toISOString(), ...f })),
};

/**
 * Runs Document AI OCR on a PDF buffer synchronously.
 * Throws with specific error codes so the controller can
 * return the right message to the teacher.
 *
 * Error codes:
 *   OCR_EMPTY      — Doc AI returned no text (image-locked PDF)
 *   OCR_TIMEOUT    — Doc AI took too long
 *   OCR_API_ERROR  — Doc AI returned an API error
 */
export async function extractTextFromBuffer(pdfBuffer) {
  if (!pdfBuffer || pdfBuffer.length === 0) {
    throw Object.assign(new Error('PDF buffer is empty'), { code: 'OCR_EMPTY' });
  }

  log.info('OCR_START', { bytes: pdfBuffer.length });

  let result;
  try {
    [result] = await withTimeout(
      client.processDocument({
        name: PROCESSOR_NAME,
        rawDocument: { content: pdfBuffer, mimeType: 'application/pdf' },
      }),
      DOC_AI_TIMEOUT_MS,
      'Document AI processDocument'
    );
  } catch (err) {
    if (err.code === 'ETIMEDOUT') {
      log.error('OCR_TIMEOUT', { bytes: pdfBuffer.length, ms: DOC_AI_TIMEOUT_MS });
      throw Object.assign(
        new Error('Document AI timed out. Please try again.'),
        { code: 'OCR_TIMEOUT' }
      );
    }
    log.error('OCR_API_ERROR', { error: err.message, grpcCode: err.code });
    throw Object.assign(
      new Error(`Document AI error: ${err.message}`),
      { code: 'OCR_API_ERROR' }
    );
  }

  const document = result?.document;

  if (!document?.text?.trim()) {
    log.warn('OCR_EMPTY_RESULT', { bytes: pdfBuffer.length });
    throw Object.assign(
      new Error('Document AI returned empty text — PDF may be image-locked or blank'),
      { code: 'OCR_EMPTY' }
    );
  }

  // ── Confidence scores ────────────────────────────────────────────────────
  const pageConfidences = (document.pages || []).map(page => {
    const tokens = page.tokens || [];
    if (!tokens.length) return null;
    const sum = tokens.reduce((acc, t) => acc + (t.layout?.confidence ?? 0), 0);
    return Number((sum / tokens.length).toFixed(4));
  }).filter(v => v !== null);

  const avgConfidence = pageConfidences.length
    ? Number((pageConfidences.reduce((a, b) => a + b, 0) / pageConfidences.length).toFixed(4))
    : null;

  // ── Layout pages ─────────────────────────────────────────────────────────
  const fullText = document.text;
  const pages = (document.pages || []).map(page => ({
    page:  page.pageNumber ?? 1,
    lines: (page.lines || []).map(line => {
      const segments = line.layout?.textAnchor?.textSegments || [];
      return segments
        .map(seg => fullText.slice(Number(seg.startIndex || 0), Number(seg.endIndex || 0)))
        .join('').replace(/\n$/, '').trim();
    }).filter(l => l.length > 0),
  }));

  log.info('OCR_COMPLETE', { textLength: fullText.length, totalPages: pages.length, avgConfidence });

  return { text: fullText, avgConfidence, pages };
}