import { Firestore } from '@google-cloud/firestore';
import { extractTextFromBuffer } from '../services/ocr.service.js';
import { parseQAFromText }       from '../services/qaParser.service.js';
import { generateEmbedding }     from '../services/embedding.service.js';
import { upsertDatapoint }       from '../services/vectorSearch.service.js';

const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7',
});

const CONFIDENCE_THRESHOLD    = 0.85;
const PER_QUESTION_TIMEOUT_MS = 90_000;
const log = {
  info:  (e, f = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event: e, ts: new Date().toISOString(), ...f })),
  warn:  (e, f = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event: e, ts: new Date().toISOString(), ...f })),
  error: (e, f = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event: e, ts: new Date().toISOString(), ...f })),
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

function buildWarnings(questions) {
  const warnings = [];

  const nullMarks = questions
    .filter(q => q.maxMarks === null)
    .map(q => q.questionNo);

  if (nullMarks.length > 0) {
    warnings.push({
      code:        'MARKS_MISSING',
      message:     `maxMarks not found for Q${nullMarks.join(', Q')} — please fill before confirming`,
      questionNos: nullMarks,
    });
  }

  const emptyAnswers = questions
    .filter(q => q.modelAnswer.length < 3)
    .map(q => q.questionNo);

  if (emptyAnswers.length > 0) {
    warnings.push({
      code:        'ANSWER_TOO_SHORT',
      message:     `Model answer looks incomplete for Q${emptyAnswers.join(', Q')}`,
      questionNos: emptyAnswers,
    });
  }

  return warnings;
}

/**
 * Embed + upsert + Firestore write for a single question.
 * Extracted so confirmQA loop can wrap it in withTimeout cleanly.
 */
async function processOneQuestion(q, datapointId, examId, subjectId) {
  const textToEmbed = `Q: ${q.question.trim()}\nA: ${q.modelAnswer.trim()}`;
  const embedding   = await generateEmbedding(textToEmbed);

  await upsertDatapoint(datapointId, embedding, examId, subjectId);

  await firestore.collection('qaMetadata').doc(datapointId).set({
    datapointId,
    examId,
    subjectId,
    questionNo:  q.questionNo,
    section:     q.section    ?? null,
    question:    q.question.trim(),
    modelAnswer: q.modelAnswer.trim(),
    maxMarks:    q.maxMarks,
    embeddedAt:  new Date().toISOString(),
  }, { merge: true });
}

// ─── POST /api/v1/qa/ingest-pdf ───────────────────────────────────────────────

export async function ingestPdf(req, res) {
  const startedAt = Date.now();
  const { examId, subjectId } = req.body;

  // ── Guards ──────────────────────────────────────────────────────────────
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'PDF file is required' });
  }
  if (!examId?.trim()) {
    return res.status(400).json({ success: false, message: 'examId is required' });
  }
  if (!subjectId?.trim()) {
    return res.status(400).json({ success: false, message: 'subjectId is required' });
  }

  const jobId = `${examId}_${subjectId}_${Date.now()}`;
  log.info('QA_INGEST_PDF_START', { jobId, examId, subjectId, bytes: req.file.size });

  // ── Step 1: OCR ──────────────────────────────────────────────────────────
  let ocrResult;
  try {
    ocrResult = await extractTextFromBuffer(req.file.buffer);
  } catch (err) {
    log.error('QA_OCR_FAILED', { jobId, error: err.message, code: err.code });

    const message = {
      OCR_EMPTY:   'PDF appears to be empty or image-locked. Please upload a text-based PDF.',
      OCR_TIMEOUT: 'OCR timed out. Please try again.',
    }[err.code] ?? 'OCR processing failed. Please try again.';

    return res.status(500).json({ success: false, message });
  }

  // ── Step 2: Confidence gate ──────────────────────────────────────────────
  if (ocrResult.avgConfidence !== null && ocrResult.avgConfidence < CONFIDENCE_THRESHOLD) {
    log.warn('QA_LOW_CONFIDENCE', { jobId, avgConfidence: ocrResult.avgConfidence });
    return res.status(422).json({
      success:       false,
      message:       `PDF quality too low (confidence: ${ocrResult.avgConfidence}). Please upload a properly exported digital PDF, not a scanned image.`,
      avgConfidence: ocrResult.avgConfidence,
    });
  }

  // ── Step 3: Gemini Q&A parse ─────────────────────────────────────────────
  const { questions, parseError, rawError } = await parseQAFromText(ocrResult.text);

  if (parseError || questions.length === 0) {
    log.warn('QA_PARSE_FAILED_OR_EMPTY', { jobId, rawError });
    return res.status(200).json({
      success:       false,
      parseError:    true,
      message:       'Could not auto-detect questions. Please review the raw text and enter questions manually.',
      rawText:       ocrResult.text,
      avgConfidence: ocrResult.avgConfidence,
    });
  }

  // ── Step 4: Build warnings ───────────────────────────────────────────────
  const warnings = buildWarnings(questions);

  log.info('QA_INGEST_PDF_COMPLETE', {
    jobId,
    questionsFound: questions.length,
    warnings:       warnings.length,
    avgConfidence:  ocrResult.avgConfidence,
    ms:             Date.now() - startedAt,
  });

  // Return to teacher for preview — nothing stored yet
  return res.status(200).json({
    success:        true,
    jobId,
    examId,
    subjectId,
    questionsFound: questions.length,
    avgConfidence:  ocrResult.avgConfidence,
    warnings,
    questions,
  });
}

// ─── POST /api/v1/qa/confirm ──────────────────────────────────────────────────

export async function confirmQA(req, res) {
  const startedAt = Date.now();
  const { examId, subjectId, questions } = req.body;

  // ── Guards ──────────────────────────────────────────────────────────────
  if (!examId?.trim()) {
    return res.status(400).json({ success: false, message: 'examId is required' });
  }
  if (!subjectId?.trim()) {
    return res.status(400).json({ success: false, message: 'subjectId is required' });
  }
  if (!Array.isArray(questions) || questions.length === 0) {
    return res.status(400).json({ success: false, message: 'questions array is required' });
  }

  const missingMarks = questions.filter(q => !q.maxMarks || q.maxMarks < 1);
  if (missingMarks.length > 0) {
    return res.status(400).json({
      success: false,
      message: `maxMarks missing or invalid for Q${missingMarks.map(q => q.questionNo).join(', Q')}`,
    });
  }

  log.info('QA_CONFIRM_START', { examId, subjectId, total: questions.length });

  // ── Embed + store loop ───────────────────────────────────────────────────
  const results  = [];
  let   succeeded = 0;

  for (const q of questions) {
    const datapointId = `${examId}_${subjectId}_q${q.questionNo}`;

    try {
      await withTimeout(
        processOneQuestion(q, datapointId, examId, subjectId),
        PER_QUESTION_TIMEOUT_MS,
        `question ${q.questionNo}`
      );

      log.info('QA_CONFIRM_ITEM_OK', { datapointId });
      results.push({ datapointId, questionNo: q.questionNo, success: true });
      succeeded++;

    } catch (err) {
      log.error('QA_CONFIRM_ITEM_FAILED', { datapointId, error: err.message, code: err.code });
      results.push({ datapointId, questionNo: q.questionNo, success: false, error: err.message });
    }
  }

  // ── Write examIndex ──────────────────────────────────────────────────────
  try {
    await firestore.collection('examIndex').doc(`${examId}_${subjectId}`).set({
      examId,
      subjectId,
      totalQuestions:  questions.length,
      ingestedCount:   succeeded,
      failedQuestions: results.filter(r => !r.success).map(r => r.questionNo),
      confirmedAt:     new Date().toISOString(),
      processingMs:    Date.now() - startedAt,
    }, { merge: true });
  } catch (err) {
    // Non-fatal — exam answers are stored, only index record failed
    log.warn('QA_EXAM_INDEX_WRITE_FAILED', { examId, subjectId, error: err.message });
  }

  log.info('QA_CONFIRM_COMPLETE', {
    examId, subjectId, succeeded, total: questions.length, ms: Date.now() - startedAt,
  });

  return res.status(207).json({
    success:   succeeded === questions.length,
    examId,
    subjectId,
    succeeded,
    failed:    questions.length - succeeded,
    results,
  });
}