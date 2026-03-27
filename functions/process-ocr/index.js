'use strict';

const path      = require('path');
const functions = require('@google-cloud/functions-framework');
const { Storage }   = require('@google-cloud/storage');
const { PubSub }    = require('@google-cloud/pubsub');
const { Firestore } = require('@google-cloud/firestore');

const { runInlineOcr }                           = require('./inlineOcr');
const { submitBatchOcr, buildBatchOutputPrefix } = require('./batchOcr');

// ─── Config ───────────────────────────────────────────────────────────────────
const TOPIC_NAME = process.env.OCR_TOPIC     || 'exam-ocr-completed';
const projectId  = process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7';

// 15 pages tak inline — uske upar batch
// Inline limit: ~15 pages / ~18MB whichever comes first
const INLINE_PAGE_LIMIT = parseInt(process.env.INLINE_PAGE_LIMIT || '15', 10);
const MAX_FILE_BYTES    = 18 * 1024 * 1024;

const MIME_TYPES = {
  pdf:  'application/pdf',
  jpg:  'image/jpeg',
  jpeg: 'image/jpeg',
  png:  'image/png',
};

// ─── Clients ──────────────────────────────────────────────────────────────────
const storage   = new Storage();
const pubsub    = new PubSub();
const firestore = new Firestore({ projectId });

// ─── Logger ───────────────────────────────────────────────────────────────────
const log = {
  info:  (event, fields = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event, ...fields })),
  warn:  (event, fields = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event, ...fields })),
  error: (event, fields = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event, ...fields })),
};

// ─── Helpers ──────────────────────────────────────────────────────────────────
function buildOcrOutputPath(filePath) {
  const dir  = path.posix.dirname(filePath);
  const base = path.posix.basename(filePath, path.posix.extname(filePath));
  return `${dir}/${base}-ocr.json`;
}

function shouldSkipFile(filePath, customMetadata = {}) {
  if (!filePath)                              return true;
  if (filePath.endsWith('-ocr.json'))         return true;
  if (filePath.startsWith('ocr-batch-output')) return true;  // batch output ignore karo
  if (customMetadata.ocrGenerated === 'true') return true;
  return false;
}

async function getFileMetadata(bucketName, filePath) {
  const [meta] = await storage.bucket(bucketName).file(filePath).getMetadata();
  return meta?.metadata || {};
}

async function markJobFailed(jobId, reason) {
  if (!jobId) return;
  try {
    await firestore.collection('exam_jobs').doc(jobId).update({
      status:   'failed',
      error:    reason,
      failedAt: new Date().toISOString(),
    });
  } catch (err) {
    log.warn('JOB_STATUS_UPDATE_FAILED', { jobId, reason, error: err.message });
  }
}

/**
 * PDF ka page count sirf header bytes padhke nikalta hai.
 * Full download avoid karta hai — fast aur cheap.
 */
async function getPdfPageCount(bucketName, filePath) {
  try {
    // PDF spec: page count /Count keyword ke baad hota hai
    // Sirf first 32KB padhte hain — almost always enough
    const [partialBuffer] = await storage
      .bucket(bucketName)
      .file(filePath)
      .download({ start: 0, end: 32767 });

    const text = partialBuffer.toString('latin1');

    // /Count N pattern dhundo — last match lena (nested pages ke liye)
    const matches = [...text.matchAll(/\/Count\s+(\d+)/g)];
    if (matches.length > 0) {
      const count = parseInt(matches[matches.length - 1][1], 10);
      if (count > 0 && count < 10000) return count;
    }

    // Fallback — count nahi mila, batch use karo (safe side)
    log.warn('PDF_PAGE_COUNT_FALLBACK', { bucketName, filePath, reason: 'Count not found in first 32KB' });
    return 999;

  } catch (err) {
    log.warn('PDF_PAGE_COUNT_ERROR', { bucketName, filePath, error: err.message });
    return 999; // Error pe bhi batch safe hai
  }
}

async function publishToPubSub(message, filePath, bucketName) {
  const MAX_ATTEMPTS = 3;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await pubsub
        .topic(TOPIC_NAME)
        .publishMessage({ data: Buffer.from(JSON.stringify(message)) });
      return true;
    } catch (err) {
      log.warn('OCR_PUBSUB_PUBLISH_FAILED', {
        file: filePath, attempt, maxAttempts: MAX_ATTEMPTS, error: err.message,
      });
      if (attempt < MAX_ATTEMPTS) {
        await new Promise(r => setTimeout(r, 500 * attempt));
      }
    }
  }
  return false;
}

// ─── Main handler ─────────────────────────────────────────────────────────────
functions.cloudEvent('processOCR', async (cloudEvent) => {
  const fileData   = cloudEvent.data || {};
  const bucketName = fileData.bucket;
  const filePath   = fileData.name;

  if (!bucketName || !filePath) {
    log.warn('OCR_SKIP_INVALID_EVENT', { reason: 'missing bucket or file name' });
    return;
  }

  const extension = filePath.split('.').pop().toLowerCase();
  const mimeType  = MIME_TYPES[extension];
  if (!mimeType) {
    log.info('OCR_SKIP_UNSUPPORTED_TYPE', { bucket: bucketName, file: filePath, extension });
    return;
  }

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

  const jobId = customMetadata.jobid || null;

  log.info('OCR_PROCESS_START', { bucket: bucketName, file: filePath, mimeType, jobId });

  // ── Image files — inline only (page count irrelevant) ─────────────────────
  if (extension !== 'pdf') {
    await handleInline({ bucketName, filePath, mimeType, jobId, customMetadata });
    return;
  }

  // ── PDF — page count check karo ───────────────────────────────────────────
  const pageCount = await getPdfPageCount(bucketName, filePath);

  log.info('PDF_PAGE_COUNT', { bucket: bucketName, file: filePath, pageCount, inlineLimit: INLINE_PAGE_LIMIT });

  if (pageCount <= INLINE_PAGE_LIMIT) {
    await handleInline({ bucketName, filePath, mimeType, jobId, customMetadata });
  } else {
    await handleBatch({ bucketName, filePath, mimeType, jobId, customMetadata, pageCount });
  }
});

// ─── Inline handler ───────────────────────────────────────────────────────────
async function handleInline({ bucketName, filePath, mimeType, jobId, customMetadata }) {
  log.info('OCR_INLINE_START', { bucket: bucketName, file: filePath });

  // Download
  const [fileBuffer] = await storage.bucket(bucketName).file(filePath).download();

  if (fileBuffer.length > MAX_FILE_BYTES) {
    log.error('OCR_FILE_TOO_LARGE_FOR_INLINE', {
      bucket: bucketName, file: filePath,
      sizeMB: (fileBuffer.length / 1024 / 1024).toFixed(1),
    });
    // File size badi hai lekin pages kam — batch pe route karo
    log.info('OCR_ROUTING_TO_BATCH_FALLBACK', { bucket: bucketName, file: filePath });
    await handleBatch({ bucketName, filePath, mimeType, jobId, customMetadata, pageCount: null });
    return;
  }

  let ocrPayload;
  try {
    ocrPayload = await runInlineOcr({ fileBuffer, mimeType, filePath, bucketName, jobId, customMetadata });
  } catch (err) {
    log.error('OCR_INLINE_FAILED', { bucket: bucketName, file: filePath, error: err.message, code: err.code });
    await markJobFailed(jobId, `Inline OCR failed: ${err.message}`);
    return;
  }

  await saveAndPublish({ ocrPayload, filePath, bucketName, jobId, customMetadata, mode: 'inline' });
}

// ─── Batch handler ────────────────────────────────────────────────────────────
async function handleBatch({ bucketName, filePath, mimeType, jobId, customMetadata, pageCount }) {
  log.info('OCR_BATCH_SUBMIT_START', { bucket: bucketName, file: filePath, pageCount });

  let operationName, outputPrefix, outputBucket;

  try {
    ({ operationName, outputPrefix, outputBucket } = await submitBatchOcr({
      filePath, bucketName, mimeType,
    }));
  } catch (err) {
    log.error('OCR_BATCH_SUBMIT_FAILED', {
      bucket: bucketName, file: filePath, error: err.message, code: err.code,
    });
    await markJobFailed(jobId, `Batch OCR submit failed: ${err.message}`);
    return;
  }

  // Firestore mein operation track karo — worker poll karega
  try {
    await firestore.collection('ocr_batch_operations').doc(operationName.split('/').pop()).set({
      operationName,
      outputBucket,
      outputPrefix,
      sourceFile:   filePath,
      sourceBucket: bucketName,
      jobId,
      examId:       customMetadata.examid    || null,
      subjectId:    customMetadata.subjectid || null,
      student: {
        schoolName: customMetadata.schoolname || null,
        branchId:   customMetadata.branchid   || null,
        classId:    customMetadata.classid    || null,
        sectionId:  customMetadata.sectionid  || null,
        studentId:  customMetadata.studentid  || null,
      },
      customMetadata,
      pageCount:    pageCount ?? null,
      status:       'pending',
      submittedAt:  new Date().toISOString(),
      pollAttempts: 0,
    });
  } catch (err) {
    log.error('OCR_BATCH_RECORD_FAILED', { operationName, error: err.message });
    await markJobFailed(jobId, `Failed to record batch operation: ${err.message}`);
    return;
  }

  // Job status → batch processing mein hai
  if (jobId) {
    await firestore.collection('exam_jobs').doc(jobId).update({
      status:        'ocr_batch_pending',
      operationName,
      batchSubmittedAt: new Date().toISOString(),
    }).catch(err => log.warn('JOB_BATCH_STATUS_UPDATE_FAILED', { jobId, error: err.message }));
  }

  log.info('OCR_BATCH_SUBMITTED', {
    bucket: bucketName, file: filePath, operationName, outputPrefix, jobId,
  });
}

// ─── Save OCR JSON + Publish ───────────────────────────────────────────────────
// Inline aur batch dono ke liye same final step
async function saveAndPublish({ ocrPayload, filePath, bucketName, jobId, customMetadata, mode }) {
  const outputPath = buildOcrOutputPath(filePath);

  // GCS mein save karo
  await storage.bucket(bucketName).file(outputPath).save(
    JSON.stringify(ocrPayload, null, 2),
    {
      contentType: 'application/json',
      metadata: {
        metadata: {
          ocrGenerated: 'true',
          jobId:        jobId ?? '',
          sourceFile:   filePath,
          processingMode: mode,
          ...ocrPayload.student,
        },
      },
    }
  );

  log.info('OCR_OUTPUT_SAVED', {
    bucket: bucketName, outputPath,
    totalPages: ocrPayload.totalPages,
    textLength: ocrPayload.text.length,
    mode,
  });

  // Source file mark karo
  try {
    await storage.bucket(bucketName).file(filePath).setMetadata({
      metadata: { ocrGenerated: 'true' },
    });
  } catch (err) {
    log.warn('OCR_SOURCE_MARK_FAILED', { bucket: bucketName, file: filePath, error: err.message });
  }

  // Pub/Sub publish
  const message = {
    bucket:      bucketName,
    ocrPath:     outputPath,
    sourceFile:  filePath,
    jobId,
    examId:      customMetadata.examid    || null,
    subjectId:   customMetadata.subjectid || null,
    generatedAt: ocrPayload.generatedAt,
    student:     ocrPayload.student,
  };

  const published = await publishToPubSub(message, filePath, bucketName);

  if (published) {
    log.info('OCR_PUBSUB_PUBLISHED', { bucket: bucketName, topic: TOPIC_NAME, outputPath, mode });
  } else {
    log.error('OCR_PUBSUB_EXHAUSTED', {
      bucket: bucketName, file: filePath, outputPath, mode,
      hint: 'OCR JSON saved in GCS — trigger manually if needed',
    });
    await markJobFailed(jobId, `Pub/Sub publish failed — OCR saved at ${outputPath}`);
  }
}