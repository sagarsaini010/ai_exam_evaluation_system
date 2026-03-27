'use strict';

/**
 * Batch OCR — >15 pages ke liye.
 * Document AI ko GCS input/output deta hai — async operation.
 */

const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { Storage } = require('@google-cloud/storage');
const path = require('path');

const location    = process.env.DOCUMENT_AI_LOCATION     || 'asia-south1';
const projectId   = process.env.GCP_PROJECT_ID           || 'secure-brook-470609-q7';
const processorId = process.env.DOCUMENT_AI_PROCESSOR_ID || 'f9b5a9f31d819f11';
const bucket      = process.env.CENTRAL_BUCKET           || 'ai-exam-storage-470609-q7';

const docAIClient = new DocumentProcessorServiceClient({
  apiEndpoint: `${location}-documentai.googleapis.com`,
});
const storage = new Storage();

const processorName = `projects/${projectId}/locations/${location}/processors/${processorId}`;

/**
 * Batch operation output GCS path banata hai.
 * e.g. school/branch/.../123-answer.pdf
 *   →  ocr-batch-output/school/branch/.../123-answer/
 */
function buildBatchOutputPrefix(filePath) {
  const dir  = path.posix.dirname(filePath);
  const base = path.posix.basename(filePath, path.posix.extname(filePath));
  return `ocr-batch-output/${dir}/${base}/`;
}

/**
 * Batch OCR submit karta hai — operation name return karta hai.
 * Actual processing async hoti hai (2-10 min for 30-50 pages).
 */
async function submitBatchOcr({ filePath, bucketName, mimeType }) {
  const outputPrefix = buildBatchOutputPrefix(filePath);

  // ✅ batchProcessDocuments returns [operation, initialApiResponse]
  const [operation] = await docAIClient.batchProcessDocuments({
    name: processorName,
    inputDocuments: {
      gcsDocuments: {
        documents: [{
          gcsUri:   `gs://${bucketName}/${filePath}`,
          mimeType,
        }],
      },
    },
    documentOutputConfig: {
      gcsOutputConfig: {
        gcsUri: `gs://${bucket}/${outputPrefix}`,
        fieldMask: {
          paths: ['text', 'pages.page_number', 'pages.lines', 'pages.tokens'],
        },
      },
    },
    individualProcessTimeout: { seconds: 300 },
  });

  // ✅ LRO name operation.name pe hota hai
  const operationName = operation.name;

  if (!operationName) {
    throw Object.assign(
      new Error('Batch OCR operation name missing from Document AI response'),
      { code: 'BATCH_OP_NAME_MISSING' }
    );
  }

  return {
    operationName,
    outputPrefix,
    outputBucket: bucket,
  };
}

/**
 * Batch operation ka status check karta hai.
 * Returns: { done, error, metadata }
 */
async function checkBatchOperation(operationName) {
  const operation = await docAIClient.checkBatchProcessDocumentsProgress(operationName);

  return {
    done:           operation.done ?? false,
    error:          operation.error ?? null,
    // Progress metadata (individual document status)
    individualProcessStatuses: operation.metadata?.individualProcessStatuses ?? [],
  };
}

/**
 * Batch output GCS se padhta hai aur inline OCR jaisa ocrPayload banata hai.
 * Document AI batch output multiple JSON shards mein hota hai.
 */
async function parseBatchOutput({ outputBucket, outputPrefix, filePath, jobId, customMetadata }) {
  // Output folder ke saare JSON files list karo
  const [files] = await storage.bucket(outputBucket).getFiles({ prefix: outputPrefix });

  const jsonFiles = files.filter(f => f.name.endsWith('.json'));

  if (jsonFiles.length === 0) {
    throw Object.assign(
      new Error(`No batch output JSON found at gs://${outputBucket}/${outputPrefix}`),
      { code: 'BATCH_OUTPUT_MISSING' }
    );
  }

  // Saare shards merge karo (usually ek hi hota hai for single document)
  let mergedText     = '';
  let allPages       = [];
  let allConfidences = [];

  for (const file of jsonFiles) {
    const [content] = await file.download();
    const doc = JSON.parse(content.toString());

    if (!doc) continue;

    const fullText = doc.text || '';
    mergedText += fullText;

    // Pages parse karo — same logic as inline
    const pages = (doc.pages || []).map((page) => {
      const pageNumber = page.pageNumber ?? 1;

      const lines = (page.lines || []).map((line) => {
        const segments = line.layout?.textAnchor?.textSegments || [];
        return segments
          .map((seg) => {
            const start = Number(seg.startIndex || 0);
            const end   = Number(seg.endIndex   || 0);
            return fullText.slice(start, end);
          })
          .join('')
          .replace(/\n$/, '')
          .trim();
      }).filter(l => l.length > 0);

      const tokens = page.tokens || [];
      let pageConfidence = null;
      if (tokens.length > 0) {
        const sum = tokens.reduce((acc, t) => acc + (t.layout?.confidence ?? 0), 0);
        pageConfidence = Number((sum / tokens.length).toFixed(4));
      }

      allConfidences.push(pageConfidence);
      return { page: pageNumber, confidence: pageConfidence, lines };
    });

    allPages.push(...pages);
  }

  // Sort pages by page number (shards mein order guarantee nahi)
  allPages.sort((a, b) => a.page - b.page);

  const validConfidences = allConfidences.filter(v => v !== null);
  const avgConfidence = validConfidences.length > 0
    ? Number((validConfidences.reduce((a, b) => a + b, 0) / validConfidences.length).toFixed(4))
    : null;

  return {
    sourceFile:     filePath,
    bucket:         outputBucket,
    jobId,
    text:           mergedText,
    totalPages:     allPages.length,
    avgConfidence,
    pageConfidence: validConfidences,
    pages:          allPages,
    generatedAt:    new Date().toISOString(),
    examId:         customMetadata.examid    || null,
    subjectId:      customMetadata.subjectid || null,
    processingMode: 'batch',
    student: {
      schoolName: customMetadata.schoolname || null,
      branchId:   customMetadata.branchid   || null,
      classId:    customMetadata.classid    || null,
      sectionId:  customMetadata.sectionid  || null,
      studentId:  customMetadata.studentid  || null,
    },
  };
}

module.exports = { submitBatchOcr, checkBatchOperation, parseBatchOutput, buildBatchOutputPrefix };