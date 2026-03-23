import { GoogleGenerativeAI } from '@google/generative-ai';
import { Firestore }          from '@google-cloud/firestore';
import { generateEmbedding }  from './embedding.service.js';
import { queryNearest }       from './vectorSearch.service.js';
import {
  preGradingCheck,
  detectAnswerType,
  buildFlags,
  aggregateResults,
} from './gradingUtils.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({
  model:            'gemini-3.1-flash-lite-preview',
  generationConfig: { temperature: 0 },
});

const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7',
});

const log = {
  info:  (e, f = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event: e, ts: new Date().toISOString(), ...f })),
  warn:  (e, f = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event: e, ts: new Date().toISOString(), ...f })),
  error: (e, f = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event: e, ts: new Date().toISOString(), ...f })),
};

const RETRY_CONFIG = { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10000 };

function withTimeout(promise, ms, label = 'operation') {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(
        Object.assign(new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: 'ETIMEDOUT' })
      ), ms)
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

// ─── Grading prompt builder ───────────────────────────────────────────────────

function buildGradingPrompt(question, modelAnswer, studentAnswer, maxMarks, answerType) {

  const typeInstruction = {
    visual: `NOTE: This question requires a diagram/map/table.
OCR may have missed visual content. Grade only the written explanation.
Do NOT give 0 just because diagram is not visible in text — check written description.`,

    formula: `NOTE: This is a numerical/formula-based question.
Check BOTH the method/steps AND the final answer.
Award partial marks if method is correct but final answer has a calculation error.
Accept equivalent mathematical expressions.`,

    mixed: `NOTE: This question has both visual and formula components.
Grade the written explanation and numerical work.
Do not penalise for missing diagram in OCR.`,

    text: '',
  }[answerType] ?? '';

  return `You are an expert CBSE Class 10 exam evaluator for Indian school students.
Evaluate the student answer against the model answer and assign marks fairly.

${typeInstruction}

QUESTION:
${question}

MODEL ANSWER (full marks answer):
${modelAnswer}

STUDENT ANSWER:
${studentAnswer}

MAX MARKS: ${maxMarks}

EVALUATION RULES:
1. Award marks based on KEY POINTS covered — not exact wording.
2. Partial credit is allowed — proportional to points covered.
3. Accept correct answers written differently or in different order.
4. Accept both Hindi and English answers.
5. Introduction and Conclusion carry marks if relevant and present.
6. Do not penalise for minor spelling mistakes.
7. Be fair but strict — vague or incorrect statements get no marks.
8. marksAwarded must be between 0 and ${maxMarks}.

Return ONLY this JSON — no explanation, no markdown, no extra text:
{
  "marksAwarded": <number 0 to ${maxMarks}>,
  "feedback": "<2-3 lines in English — what student did well, what was missing>",
  "keyPointsCovered": ["<point1>", "<point2>"],
  "keyPointsMissed": ["<point1>", "<point2>"],
  "confidence": "<high|medium|low>"
}`;
}

// ─── Single question Gemini grading ──────────────────────────────────────────

async function callGeminiGrading(question, modelAnswer, studentAnswer, maxMarks, answerType) {
  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = Math.min(
          RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 500,
          RETRY_CONFIG.maxDelayMs
        );
        log.warn('GRADING_RETRY', { attempt, error: lastError?.message });
        await sleep(delay);
      }

      const prompt = buildGradingPrompt(question, modelAnswer, studentAnswer, maxMarks, answerType);
      const result = await withTimeout(
        model.generateContent(prompt),
        60_000,
        'Gemini grading'
      );

      const raw     = result.response.text();
      const cleaned = raw.replace(/```json|```/gi, '').trim();

      let parsed;
      try {
        parsed = JSON.parse(cleaned);
      } catch {
        const start = cleaned.indexOf('{');
        const end   = cleaned.lastIndexOf('}');
        if (start !== -1 && end > start) {
          parsed = JSON.parse(cleaned.slice(start, end + 1));
        } else {
          throw Object.assign(new Error('Non-JSON response'), { nonRetryable: true });
        }
      }

      // Sanitize — marks must be within bounds
      const marksAwarded = Math.min(
        Math.max(0, Number(parsed.marksAwarded ?? 0)),
        maxMarks
      );

      return {
        marksAwarded,
        feedback:         String(parsed.feedback ?? '').trim(),
        keyPointsCovered: Array.isArray(parsed.keyPointsCovered) ? parsed.keyPointsCovered : [],
        keyPointsMissed:  Array.isArray(parsed.keyPointsMissed)  ? parsed.keyPointsMissed  : [],
        confidence:       ['high', 'medium', 'low'].includes(parsed.confidence)
                            ? parsed.confidence : 'medium',
      };

    } catch (err) {
      lastError = err;
      if (err.nonRetryable || !isRetryable(err)) {
        log.error('GRADING_NON_RETRYABLE', { attempt: attempt + 1, error: err.message });
        break;
      }
      log.warn('GRADING_RETRYABLE', { attempt: attempt + 1, error: err.message });
    }
  }

  // Gemini fail hua — fallback result return karo
  log.error('GRADING_ALL_ATTEMPTS_FAILED', { error: lastError?.message });
  return {
    marksAwarded:     0,
    feedback:         'Grading could not be completed automatically. Please review manually.',
    keyPointsCovered: [],
    keyPointsMissed:  [],
    confidence:       'low',
    gradingFailed:    true,
  };
}

// ─── Main export — grade all segmented answers ────────────────────────────────

/**
 * Saare segmented answers ko grade karo.
 * Sequential processing with delays to respect API quotas.
 *
 * @param {Array}  segmentedQuestions   worker ke segmentedAnswers.questions
 * @param {string} examId
 * @param {string} subjectId
 * @param {object} student              { schoolName, branchId, classId, sectionId, studentId }
 * @param {string} jobId
 * @returns {object}  complete grading result
 */
export async function gradeAllAnswers(segmentedQuestions, examId, subjectId, student, jobId) {
  const startedAt    = Date.now();
  const gradedAnswers = [];

  log.info('GRADING_START', {
    jobId,
    examId,
    subjectId,
    totalQuestions: segmentedQuestions.length,
  });

  for (const q of segmentedQuestions) {
    const questionNumber = q.questionNumber;
    const studentAnswer  = (q.answer ?? '').trim();
    const questionText   = (q.question ?? '').trim();

    log.info('GRADING_QUESTION_START', { jobId, questionNumber });

    // ── Pre-grading check ──────────────────────────────────────────────────
    const { skip, skipReason, marksAwarded: skipMarks } = preGradingCheck(studentAnswer);

    if (skip) {
      log.info('GRADING_QUESTION_SKIPPED', { jobId, questionNumber, skipReason });
      gradedAnswers.push({
        questionNumber,
        studentAnswer,
        question:         null,
        modelAnswer:      null,
        maxMarks:         null,
        marksAwarded:     skipMarks,
        marksOverride:    null,
        feedback:         `Answer was ${skipReason === 'blank_answer' ? 'blank' : 'too short'}.`,
        keyPointsCovered: [],
        keyPointsMissed:  [],
        confidence:       'high',
        answerType:       'text',
        matchDistance:    null,
        datapointId:      null,
        gradingSkipped:   true,
        skipReason,
        flags:            buildFlags({ skipReason, isUnmatched: false }),
      });
      continue;
    }

    // ── Detect answer type ─────────────────────────────────────────────────
    const answerType = detectAnswerType(studentAnswer, questionText);

    // ── Embed student answer ───────────────────────────────────────────────
    let embedding;
    try {
      embedding = await withTimeout(
        generateEmbedding(studentAnswer, 'RETRIEVAL_QUERY'),
        60_000,
        `embed question ${questionNumber}`
      );
    } catch (err) {
      log.error('GRADING_EMBED_FAILED', { jobId, questionNumber, error: err.message });
      gradedAnswers.push({
        questionNumber,
        studentAnswer,
        question:         null,
        modelAnswer:      null,
        maxMarks:         null,
        marksAwarded:     0,
        marksOverride:    null,
        feedback:         'Embedding failed. Please review manually.',
        keyPointsCovered: [],
        keyPointsMissed:  [],
        confidence:       'low',
        answerType,
        matchDistance:    null,
        datapointId:      null,
        gradingSkipped:   true,
        skipReason:       'embedding_failed',
        flags:            ['embedding_failed'],
      });
      await sleep(2000);
      continue;
    }

    // ── Vector search — nearest Q+A pair ──────────────────────────────────
    let neighbors = [];
    try {
      neighbors = await withTimeout(
        queryNearest(embedding, examId, subjectId, 1),
        30_000,
        `vector search question ${questionNumber}`
      );
    } catch (err) {
      log.error('GRADING_VECTOR_SEARCH_FAILED', { jobId, questionNumber, error: err.message });
    }

    // No match found
    if (!neighbors.length) {
      log.warn('GRADING_NO_MATCH', { jobId, questionNumber });
      gradedAnswers.push({
        questionNumber,
        studentAnswer,
        question:         null,
        modelAnswer:      null,
        maxMarks:         null,
        marksAwarded:     0,
        marksOverride:    null,
        feedback:         'No matching question found in database. Please review manually.',
        keyPointsCovered: [],
        keyPointsMissed:  [],
        confidence:       'low',
        answerType,
        matchDistance:    null,
        datapointId:      null,
        gradingSkipped:   true,
        skipReason:       'unmatched_question',
        flags:            buildFlags({ isUnmatched: true }),
      });
      await sleep(2000);
      continue;
    }

    const { datapointId, distance: matchDistance } = neighbors[0];

    // ── Fetch model answer from Firestore ──────────────────────────────────
    let qaMetadata;
    try {
      const doc = await withTimeout(
        firestore.collection('qaMetadata').doc(datapointId).get(),
        10_000,
        `fetch qaMetadata ${datapointId}`
      );
      qaMetadata = doc.exists ? doc.data() : null;
    } catch (err) {
      log.error('GRADING_METADATA_FETCH_FAILED', { jobId, questionNumber, datapointId, error: err.message });
    }

    if (!qaMetadata) {
      log.warn('GRADING_METADATA_NOT_FOUND', { jobId, questionNumber, datapointId });
      gradedAnswers.push({
        questionNumber,
        studentAnswer,
        question:         null,
        modelAnswer:      null,
        maxMarks:         null,
        marksAwarded:     0,
        marksOverride:    null,
        feedback:         'Model answer not found. Please review manually.',
        keyPointsCovered: [],
        keyPointsMissed:  [],
        confidence:       'low',
        answerType,
        matchDistance,
        datapointId,
        gradingSkipped:   true,
        skipReason:       'model_answer_not_found',
        flags:            ['model_answer_not_found'],
      });
      await sleep(2000);
      continue;
    }

    const { question, modelAnswer, maxMarks } = qaMetadata;

    // ── Gemini grading ─────────────────────────────────────────────────────
    const gradingResult = await callGeminiGrading(
      question, modelAnswer, studentAnswer, maxMarks, answerType
    );

    // ── Build flags ────────────────────────────────────────────────────────
    const flags = buildFlags({
      matchDistance,
      marksAwarded:    gradingResult.marksAwarded,
      studentAnswer,
      answerType,
      geminiConfidence: gradingResult.confidence,
      skipReason:       null,
      isUnmatched:      false,
    });

    if (gradingResult.gradingFailed) flags.push('grading_failed');

    log.info('GRADING_QUESTION_COMPLETE', {
      jobId,
      questionNumber,
      datapointId,
      marksAwarded: gradingResult.marksAwarded,
      maxMarks,
      matchDistance: matchDistance.toFixed(4),
      flags,
    });

    gradedAnswers.push({
      questionNumber,
      studentAnswer,
      question,
      modelAnswer,
      maxMarks,
      marksAwarded:     gradingResult.marksAwarded,
      marksOverride:    null,         // teacher baad mein override kar sakta hai
      feedback:         gradingResult.feedback,
      keyPointsCovered: gradingResult.keyPointsCovered,
      keyPointsMissed:  gradingResult.keyPointsMissed,
      confidence:       gradingResult.confidence,
      answerType,
      matchDistance,
      datapointId,
      gradingSkipped:   false,
      skipReason:       null,
      flags,
    });

    // Rate limiting — 2s between questions
    await sleep(2000);
  }

  // ── Aggregate results ──────────────────────────────────────────────────────
  const { totalMarks, maxMarks, percentage, gradingStatus, flaggedQuestions } =
    aggregateResults(gradedAnswers);

  const result = {
    examId,
    subjectId,
    student,
    jobId,
    gradingStatus,
    totalMarks,
    maxMarks,
    percentage,
    flaggedQuestions,
    gradedAnswers,
    gradedAt:         new Date().toISOString(),
    processingTimeMs: Date.now() - startedAt,
  };

  log.info('GRADING_COMPLETE', {
    jobId,
    gradingStatus,
    totalMarks,
    maxMarks,
    percentage,
    flaggedCount: flaggedQuestions.length,
    processingTimeMs: result.processingTimeMs,
  });

  return result;
}