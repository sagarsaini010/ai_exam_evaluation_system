'use strict';

/**
 * Inline OCR — ≤15 pages ke liye.
 * Existing processDocument logic yahan move kiya gaya hai.
 */

const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;

const location    = process.env.DOCUMENT_AI_LOCATION     || 'asia-south1';
const projectId   = process.env.GCP_PROJECT_ID           || 'secure-brook-470609-q7';
const processorId = process.env.DOCUMENT_AI_PROCESSOR_ID || 'f9b5a9f31d819f11';

const docAIClient = new DocumentProcessorServiceClient({
  apiEndpoint: `${location}-documentai.googleapis.com`,
});

const processorName = `projects/${projectId}/locations/${location}/processors/${processorId}`;

/**
 * Document AI response se layout-aware pages extract karta hai.
 */
function extractLayoutPages(document) {
  if (!document.pages || document.pages.length === 0) return [];

  const fullText = document.text || '';

  return document.pages.map((page) => {
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
    }).filter(line => line.length > 0);

    const tokens = page.tokens || [];
    let pageConfidence = null;
    if (tokens.length > 0) {
      const sum = tokens.reduce((acc, t) => acc + (t.layout?.confidence ?? 0), 0);
      pageConfidence = Number((sum / tokens.length).toFixed(4));
    }

    return { page: pageNumber, confidence: pageConfidence, lines };
  });
}

/**
 * Confidence scores extract karta hai document se.
 */
function extractConfidence(document) {
  let pageConfidences = [];
  let avgConfidence   = null;

  if (document.pages?.length > 0) {
    pageConfidences = document.pages.map(page => {
      const tokens = page.tokens || [];
      if (tokens.length === 0) return null;
      const sum = tokens.reduce((acc, t) => acc + (t.layout?.confidence ?? 0), 0);
      return Number((sum / tokens.length).toFixed(4));
    }).filter(v => v !== null);

    if (pageConfidences.length > 0) {
      const total = pageConfidences.reduce((a, b) => a + b, 0);
      avgConfidence = Number((total / pageConfidences.length).toFixed(4));
    }
  }

  return { pageConfidences, avgConfidence };
}

/**
 * Inline OCR run karta hai — fileBuffer seedha Document AI ko deta hai.
 * Returns structured ocrPayload.
 */
async function runInlineOcr({ fileBuffer, mimeType, filePath, bucketName, jobId, customMetadata }) {
  const [result] = await docAIClient.processDocument({
    name:        processorName,
    rawDocument: { content: fileBuffer, mimeType },
  });

  const document = result.document;
  if (!document) {
    throw Object.assign(new Error('Document AI returned empty response'), { code: 'EMPTY_RESPONSE' });
  }

  const { pageConfidences, avgConfidence } = extractConfidence(document);
  const layoutPages = extractLayoutPages(document);

  return {
    sourceFile:     filePath,
    bucket:         bucketName,
    jobId,
    text:           document.text || '',
    totalPages:     document.pages?.length || 0,
    avgConfidence,
    pageConfidence: pageConfidences,
    pages:          layoutPages,
    generatedAt:    new Date().toISOString(),
    examId:         customMetadata.examid    || null,
    subjectId:      customMetadata.subjectid || null,
    processingMode: 'inline',
    student: {
      schoolName: customMetadata.schoolname || null,
      branchId:   customMetadata.branchid   || null,
      classId:    customMetadata.classid    || null,
      sectionId:  customMetadata.sectionid  || null,
      studentId:  customMetadata.studentid  || null,
    },
  };
}

module.exports = { runInlineOcr, extractLayoutPages, extractConfidence };