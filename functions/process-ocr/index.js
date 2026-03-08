const path = require('path');
const functions = require('@google-cloud/functions-framework');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');

// ─── Config ───────────────────────────────────────────────────────────────────
const TOPIC_NAME   = process.env.OCR_TOPIC                || 'exam-ocr-completed';
const projectId    = process.env.GCP_PROJECT_ID           || 'secure-brook-470609-q7';
const location     = process.env.DOCUMENT_AI_LOCATION     || 'asia-south1';
const processorId  = process.env.DOCUMENT_AI_PROCESSOR_ID || 'f9b5a9f31d819f11';

// Document AI inline limit is 20 MB — stay safely under it
const MAX_FILE_BYTES = 18 * 1024 * 1024;

const MIME_TYPES = {
  pdf:  'application/pdf',
  jpg:  'image/jpeg',
  jpeg: 'image/jpeg',
  png:  'image/png',
};

// ─── Structured logger ────────────────────────────────────────────────────────
// Cloud Logging parses JSON lines automatically — filter by event/severity/file
// in the GCP console instead of grepping free-text strings.
const log = {
  info:  (event, fields = {}) => console.log(JSON.stringify({ severity: 'INFO',    event, ...fields })),
  warn:  (event, fields = {}) => console.warn(JSON.stringify({ severity: 'WARNING', event, ...fields })),
  error: (event, fields = {}) => console.error(JSON.stringify({ severity: 'ERROR',  event, ...fields })),
};

// ─── Clients (ADC — no key file needed on Cloud Run / GCF) ───────────────────
const docAIClient = new DocumentProcessorServiceClient({
  apiEndpoint: `${location}-documentai.googleapis.com`,
});
const storage = new Storage();
const pubsub   = new PubSub();

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Derives the OCR output path from the source file path.
 * e.g. school/branch/class/section/student/123-answer.pdf
 *   →  school/branch/class/section/student/123-answer-ocr.json
 */
function buildOcrOutputPath(filePath) {
  const dir  = path.posix.dirname(filePath);   // never '.' for structured uploads
  const base = path.posix.basename(filePath, path.posix.extname(filePath));
  return `${dir}/${base}-ocr.json`;
}

/**
 * Returns true if this GCS event should be ignored:
 * - Already an OCR output file
 * - Already processed (flag written by this function)
 */
function shouldSkipFile(filePath, customMetadata = {}) {
  if (!filePath)                              return true;
  if (filePath.endsWith('-ocr.json'))         return true;
  if (customMetadata.ocrGenerated === 'true') return true;
  return false;
}

/**
 * Fetches the GCS object's custom metadata explicitly.
 * GCS event payloads do NOT reliably include custom metadata,
 * so we always do a separate getMetadata() call.
 */
async function getFileMetadata(bucketName, filePath) {
  const [meta] = await storage.bucket(bucketName).file(filePath).getMetadata();
  return meta?.metadata || {};   // custom metadata lives under .metadata
}

// ─── Main handler ─────────────────────────────────────────────────────────────

functions.cloudEvent('processOCR', async (cloudEvent) => {
  const fileData   = cloudEvent.data || {};
  const bucketName = fileData.bucket;
  const filePath   = fileData.name;

  // Basic guards
  if (!bucketName || !filePath) {
    log.warn('OCR_SKIP_INVALID_EVENT', { reason: 'missing bucket or file name' });
    return;
  }

  // Check extension early — avoids a GCS fetch for unsupported files
  const extension = filePath.split('.').pop().toLowerCase();
  const mimeType  = MIME_TYPES[extension];
  if (!mimeType) {
    log.info('OCR_SKIP_UNSUPPORTED_TYPE', { bucket: bucketName, file: filePath, extension });
    return;
  }

  // Fetch real custom metadata from GCS (event payload is not reliable)
  let customMetadata = {};
  try {
    customMetadata = await getFileMetadata(bucketName, filePath);
  } catch (err) {
    log.warn('OCR_METADATA_FETCH_FAILED', { bucket: bucketName, file: filePath, error: err.message });
  }

  if (shouldSkipFile(filePath, customMetadata)) {
    log.info('OCR_SKIP_ALREADY_PROCESSED', { bucket: bucketName, file: filePath });
    return;
  }

  log.info('OCR_PROCESS_START', { bucket: bucketName, file: filePath, mimeType });

  // ── Download ──────────────────────────────────────────────────────────────
  const [fileBuffer] = await storage.bucket(bucketName).file(filePath).download();

  if (fileBuffer.length > MAX_FILE_BYTES) {
    log.error('OCR_SKIP_FILE_TOO_LARGE', {
      bucket: bucketName,
      file:   filePath,
      sizeMB: (fileBuffer.length / 1024 / 1024).toFixed(1),
      limitMB: 18,
      hint:   'Switch to batch processing for large files',
    });
    return;
  }

  // ── Document AI ───────────────────────────────────────────────────────────
  const processorName = `projects/${projectId}/locations/${location}/processors/${processorId}`;

  let result;
  try {
    [result] = await docAIClient.processDocument({
      name: processorName,
      rawDocument: { content: fileBuffer, mimeType },
    });
  } catch (err) {
    // Document AI errors (quota exceeded, timeout, invalid document) should NOT
    // crash the function — log clearly and return so GCF doesn't retry forever.
    log.error('OCR_DOCUMENT_AI_FAILED', {
      bucket: bucketName,
      file:   filePath,
      error:  err.message,
      code:   err.code,     // gRPC status code e.g. 4 = DEADLINE_EXCEEDED
    });
    return;
  }

  const document = result.document;
  if (!document) {
    log.error('OCR_EMPTY_RESPONSE', { bucket: bucketName, file: filePath });
    return;
  }

  // ── Save OCR output to GCS ────────────────────────────────────────────────
  const outputPath = buildOcrOutputPath(filePath);

  const ocrPayload = {
    sourceFile:  filePath,
    bucket:      bucketName,
    text:        document.text || '',
    totalPages:  document.pages?.length || 0,
    generatedAt: new Date().toISOString(),
    // Student context — written by upload API, forwarded here for the AI pipeline
    student: {
  schoolName: customMetadata.schoolname || null,
  branchId:   customMetadata.branchid   || null,
  classId:    customMetadata.classid    || null,
  sectionId:  customMetadata.sectionid  || null,
  studentId:  customMetadata.studentid  || null,
},
  };

  await storage.bucket(bucketName).file(outputPath).save(
    JSON.stringify(ocrPayload, null, 2),
    {
      contentType: 'application/json',
      metadata: {
        metadata: {
          ocrGenerated: 'true',
          sourceFile:   filePath,
          // Propagate student context so downstream tools can read it from
          // the OCR file's own metadata without parsing the JSON body
          ...ocrPayload.student,
        },
      },
    }
  );

  log.info('OCR_OUTPUT_SAVED', {
    bucket:     bucketName,
    outputPath,
    totalPages: ocrPayload.totalPages,
    textLength: ocrPayload.text.length,
  });

  // ── Publish to Pub/Sub (with retry) ──────────────────────────────────────
  const message = {
    bucket:      bucketName,
    ocrPath:     outputPath,
    sourceFile:  filePath,
    generatedAt: ocrPayload.generatedAt,
    student:     ocrPayload.student,   // AI grading worker needs this immediately
  };

  const MAX_PUBLISH_ATTEMPTS = 3;
  let published = false;

  for (let attempt = 1; attempt <= MAX_PUBLISH_ATTEMPTS; attempt++) {
    try {
      await pubsub
        .topic(TOPIC_NAME)
        .publishMessage({ data: Buffer.from(JSON.stringify(message)) });
      published = true;
      break;
    } catch (err) {
      log.warn('OCR_PUBSUB_PUBLISH_FAILED', {
        bucket:  bucketName,
        file:    filePath,
        attempt,
        maxAttempts: MAX_PUBLISH_ATTEMPTS,
        error:   err.message,
      });

      if (attempt < MAX_PUBLISH_ATTEMPTS) {
        // Exponential backoff: 500ms, 1000ms
        await new Promise((r) => setTimeout(r, 500 * attempt));
      }
    }
  }

  if (published) {
    log.info('OCR_PUBSUB_PUBLISHED', { bucket: bucketName, topic: TOPIC_NAME, ocrPath: outputPath });
  } else {
    // OCR succeeded and was saved — pipeline can recover by re-reading GCS.
    // Throwing here would cause GCF to retry the entire function including re-running OCR.
    log.error('OCR_PUBSUB_PUBLISH_EXHAUSTED', {
      bucket:  bucketName,
      file:    filePath,
      ocrPath: outputPath,
      hint:    'OCR JSON is saved in GCS. Trigger pipeline manually if needed.',
    });
  }
});
