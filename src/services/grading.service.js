import { GoogleGenerativeAI } from '@google/generative-ai';
import { Firestore }          from '@google-cloud/firestore';
import { generateEmbeddingsBatch } from './embedding.service.js';
import { queryNearest }       from './vectorSearch.service.js';
import {
  preGradingCheck,
  detectAnswerType,
  buildFlags,
  aggregateResults,
} from './gradingUtils.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({
  model:            process.env.GOOGLE_GENERATIVE_MODEL,
  generationConfig: { temperature: 0 },
});

const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
});

const log = {
  info:  (e, f = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event: e, ts: new Date().toISOString(), ...f })),
  warn:  (e, f = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event: e, ts: new Date().toISOString(), ...f })),
  error: (e, f = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event: e, ts: new Date().toISOString(), ...f })),
};

const RETRY_CONFIG = { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10000 };

// ─── Grading concurrency config ───────────────────────────────────────────────
// Gemini RPM ke hisaab se adjust karo:
//   Free tier  → GRADING_CONCURRENCY = 5
//   Paid tier  → GRADING_CONCURRENCY = 10–20
const GRADING_CONCURRENCY = parseInt(process.env.GRADING_CONCURRENCY ?? '5', 10);

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

// ─── Controlled parallel executor ─────────────────────────────────────────────
// items ko concurrency limit ke saath parallel chalata hai
// Promise.all se better — ek saath sab nahi, N ek saath

async function parallelLimit(items, concurrency, asyncFn) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const i = nextIndex++;
      try {
        results[i] = { status: 'fulfilled', value: await asyncFn(items[i], i) };
      } catch (err) {
        results[i] = { status: 'rejected', reason: err };
      }
    }
  }

  // N workers ek saath chalao
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
  return results;
}

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

// ─── Single Gemini grading call with retry ────────────────────────────────────

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
      const result = await withTimeout(model.generateContent(prompt), 60_000, 'Gemini grading');

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

      return {
        marksAwarded:     Math.min(Math.max(0, Number(parsed.marksAwarded ?? 0)), maxMarks),
        feedback:         String(parsed.feedback ?? '').trim(),
        keyPointsCovered: Array.isArray(parsed.keyPointsCovered) ? parsed.keyPointsCovered : [],
        keyPointsMissed:  Array.isArray(parsed.keyPointsMissed)  ? parsed.keyPointsMissed  : [],
        confidence:       ['high', 'medium', 'low'].includes(parsed.confidence) ? parsed.confidence : 'medium',
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

  log.error('GRADING_ALL_ATTEMPTS_FAILED', { error: lastError?.message });
  return {
    marksAwarded: 0,
    feedback:     'Grading could not be completed automatically. Please review manually.',
    keyPointsCovered: [], keyPointsMissed: [],
    confidence:   'low',
    gradingFailed: true,
  };
}

// ─── PHASE 1 — Pre-check + classify ──────────────────────────────────────────
// Saare questions scan karo — blank/short ko alag karo, baaki ready karo

function classifyQuestions(segmentedQuestions) {
  const skipped = [];
  const toProcess = [];

  for (const q of segmentedQuestions) {
    const studentAnswer = (q.answer ?? '').trim();
    const questionText  = (q.question ?? '').trim();
    const { skip, skipReason, marksAwarded: skipMarks } = preGradingCheck(studentAnswer);

    if (skip) {
      skipped.push({
        questionNumber:   q.questionNumber,
        studentAnswer,
        skipReason,
        skipMarks,
        answerType:       'text',
      });
    } else {
      toProcess.push({
        questionNumber: q.questionNumber,
        studentAnswer,
        questionText,
        answerType:     detectAnswerType(studentAnswer, questionText),
      });
    }
  }

  log.info('CLASSIFY_DONE', { total: segmentedQuestions.length, toProcess: toProcess.length, skipped: skipped.length });
  return { skipped, toProcess };
}

// ─── PHASE 2 — Batch embeddings ───────────────────────────────────────────────
// Saare valid answers ke embeddings ek saath generate karo

async function embedAllAnswers(toProcess, jobId) {
  const texts = toProcess.map(q => q.studentAnswer);

  log.info('EMBED_PHASE_START', { jobId, count: texts.length });

  // batchSize=10, delayMs=500 — embedding.service se
  const embeddingResults = await generateEmbeddingsBatch(texts, 10, 500);

  log.info('EMBED_PHASE_DONE', { jobId });

  // Har question ke saath embedding attach karo
  return toProcess.map((q, i) => ({
    ...q,
    embedding:       embeddingResults[i].success ? embeddingResults[i].embedding : null,
    embeddingFailed: !embeddingResults[i].success,
    embeddingError:  embeddingResults[i].error ?? null,
  }));
}

// ─── PHASE 3 — Parallel vector search + Firestore fetch ───────────────────────
// Saare questions ke liye ek saath vector search aur metadata fetch karo

async function fetchAllModelAnswers(withEmbeddings, examId, subjectId, jobId) {
  log.info('FETCH_PHASE_START', { jobId, count: withEmbeddings.length });

  const results = await parallelLimit(withEmbeddings, 10, async (q) => {
    // Embedding fail hua tha — skip
    if (q.embeddingFailed) {
      return { ...q, fetchFailed: true, failReason: 'embedding_failed' };
    }

    // Vector search
    let neighbors = [];
    try {
      neighbors = await withTimeout(
        queryNearest(q.embedding, examId, subjectId, 1),
        30_000,
        `vector search Q${q.questionNumber}`
      );
    } catch (err) {
      log.error('VECTOR_SEARCH_FAILED', { jobId, questionNumber: q.questionNumber, error: err.message });
      return { ...q, fetchFailed: true, failReason: 'vector_search_failed' };
    }

    if (!neighbors.length) {
      log.warn('NO_MATCH', { jobId, questionNumber: q.questionNumber });
      return { ...q, fetchFailed: true, failReason: 'unmatched_question' };
    }

    const { datapointId, distance: matchDistance } = neighbors[0];

    // Firestore fetch
    let qaMetadata = null;
    try {
      const doc = await withTimeout(
        firestore.collection('qaMetadata').doc(datapointId).get(),
        10_000,
        `firestore Q${q.questionNumber}`
      );
      qaMetadata = doc.exists ? doc.data() : null;
    } catch (err) {
      log.error('FIRESTORE_FETCH_FAILED', { jobId, questionNumber: q.questionNumber, error: err.message });
      return { ...q, datapointId, matchDistance, fetchFailed: true, failReason: 'firestore_failed' };
    }

    if (!qaMetadata) {
      return { ...q, datapointId, matchDistance, fetchFailed: true, failReason: 'model_answer_not_found' };
    }

    return {
      ...q,
      datapointId,
      matchDistance,
      fetchFailed:  false,
      question:     qaMetadata.question,
      modelAnswer:  qaMetadata.modelAnswer,
      maxMarks:     qaMetadata.maxMarks,
    };
  });

  log.info('FETCH_PHASE_DONE', { jobId });
  return results.map(r => r.status === 'fulfilled' ? r.value : { ...r, fetchFailed: true, failReason: 'unexpected_error' });
}

// ─── PHASE 4 — Parallel Gemini grading ───────────────────────────────────────
// GRADING_CONCURRENCY questions ek saath grade karo

async function gradeAllParallel(withMetadata, jobId) {
  log.info('GRADING_PHASE_START', { jobId, count: withMetadata.length, concurrency: GRADING_CONCURRENCY });

  const results = await parallelLimit(withMetadata, GRADING_CONCURRENCY, async (q) => {
    if (q.fetchFailed) return q; // pehle se fail — grading skip

    const gradingResult = await callGeminiGrading(
      q.question, q.modelAnswer, q.studentAnswer, q.maxMarks, q.answerType
    );

    const flags = buildFlags({
      matchDistance:    q.matchDistance,
      marksAwarded:     gradingResult.marksAwarded,
      studentAnswer:    q.studentAnswer,
      answerType:       q.answerType,
      geminiConfidence: gradingResult.confidence,
      skipReason:       null,
      isUnmatched:      false,
    });

    if (gradingResult.gradingFailed) flags.push('grading_failed');

    log.info('GRADING_QUESTION_COMPLETE', {
      jobId,
      questionNumber: q.questionNumber,
      marksAwarded:   gradingResult.marksAwarded,
      maxMarks:       q.maxMarks,
      matchDistance:  q.matchDistance?.toFixed(4),
      flags,
    });

    return { ...q, gradingResult, flags, gradingSkipped: false };
  });

  log.info('GRADING_PHASE_DONE', { jobId });
  return results.map(r => r.status === 'fulfilled' ? r.value : { ...r.reason, fetchFailed: true, failReason: 'grading_crashed' });
}

// ─── Result assembler ─────────────────────────────────────────────────────────
// Saare phases ke results ko final gradedAnswers format mein lao

function assembleResults(skipped, graded) {
  const gradedAnswers = [];

  // Skipped questions
  for (const q of skipped) {
    gradedAnswers.push({
      questionNumber:   q.questionNumber,
      studentAnswer:    q.studentAnswer,
      question:         null,
      modelAnswer:      null,
      maxMarks:         null,
      marksAwarded:     q.skipMarks,
      marksOverride:    null,
      feedback:         `Answer was ${q.skipReason === 'blank_answer' ? 'blank' : 'too short'}.`,
      keyPointsCovered: [],
      keyPointsMissed:  [],
      confidence:       'high',
      answerType:       'text',
      matchDistance:    null,
      datapointId:      null,
      gradingSkipped:   true,
      skipReason:       q.skipReason,
      flags:            buildFlags({ skipReason: q.skipReason, isUnmatched: false }),
    });
  }

  // Processed questions
  for (const q of graded) {
    if (q.fetchFailed) {
      // Kisi bhi phase mein fail hua
      gradedAnswers.push({
        questionNumber:   q.questionNumber,
        studentAnswer:    q.studentAnswer,
        question:         null,
        modelAnswer:      null,
        maxMarks:         null,
        marksAwarded:     0,
        marksOverride:    null,
        feedback:         failReasonToFeedback(q.failReason),
        keyPointsCovered: [],
        keyPointsMissed:  [],
        confidence:       'low',
        answerType:       q.answerType ?? 'text',
        matchDistance:    q.matchDistance ?? null,
        datapointId:      q.datapointId  ?? null,
        gradingSkipped:   true,
        skipReason:       q.failReason,
        flags:            buildFlags({ skipReason: q.failReason, isUnmatched: q.failReason === 'unmatched_question' }),
      });
    } else {
      gradedAnswers.push({
        questionNumber:   q.questionNumber,
        studentAnswer:    q.studentAnswer,
        question:         q.question,
        modelAnswer:      q.modelAnswer,
        maxMarks:         q.maxMarks,
        marksAwarded:     q.gradingResult.marksAwarded,
        marksOverride:    null,
        feedback:         q.gradingResult.feedback,
        keyPointsCovered: q.gradingResult.keyPointsCovered,
        keyPointsMissed:  q.gradingResult.keyPointsMissed,
        confidence:       q.gradingResult.confidence,
        answerType:       q.answerType,
        matchDistance:    q.matchDistance,
        datapointId:      q.datapointId,
        gradingSkipped:   false,
        skipReason:       null,
        flags:            q.flags,
      });
    }
  }

  // Question number ke order mein sort karo
  gradedAnswers.sort((a, b) => a.questionNumber - b.questionNumber);
  return gradedAnswers;
}

function failReasonToFeedback(reason) {
  return {
    embedding_failed:       'Embedding failed. Please review manually.',
    vector_search_failed:   'Vector search failed. Please review manually.',
    unmatched_question:     'No matching question found in database. Please review manually.',
    firestore_failed:       'Database fetch failed. Please review manually.',
    model_answer_not_found: 'Model answer not found. Please review manually.',
    grading_crashed:        'Grading crashed unexpectedly. Please review manually.',
  }[reason] ?? 'An unknown error occurred. Please review manually.';
}

// ─── Main export ──────────────────────────────────────────────────────────────

/**
 * Redesigned parallel grading pipeline:
 *
 *  Phase 1 — Classify   : blank/short answers ko instantly skip karo        [sync]
 *  Phase 2 — Embed      : saare answers ke embeddings batch mein generate    [parallel, N=10]
 *  Phase 3 — Fetch      : vector search + Firestore ek saath                 [parallel, N=10]
 *  Phase 4 — Grade      : Gemini grading controlled concurrency ke saath     [parallel, N=GRADING_CONCURRENCY]
 *  Phase 5 — Assemble   : results jodo aur sort karo                         [sync]
 *
 * @param {Array}  segmentedQuestions
 * @param {string} examId
 * @param {string} subjectId
 * @param {object} student
 * @param {string} jobId
 * @returns {object}
 */
export async function gradeAllAnswers(segmentedQuestions, examId, subjectId, student, jobId) {
  const startedAt = Date.now();

  log.info('GRADING_PIPELINE_START', {
    jobId, examId, subjectId,
    totalQuestions: segmentedQuestions.length,
    concurrency: GRADING_CONCURRENCY,
  });

  // ── Phase 1: Classify ──────────────────────────────────────────────────────
  const { skipped, toProcess } = classifyQuestions(segmentedQuestions);

  let gradedAnswers;

  if (toProcess.length === 0) {
    // Sab blank the — directly assemble karo
    gradedAnswers = assembleResults(skipped, []);
  } else {
    // ── Phase 2: Batch embeddings ────────────────────────────────────────────
    const withEmbeddings = await embedAllAnswers(toProcess, jobId);

    // ── Phase 3: Vector search + Firestore (parallel) ────────────────────────
    const withMetadata = await fetchAllModelAnswers(withEmbeddings, examId, subjectId, jobId);

    // ── Phase 4: Parallel Gemini grading ────────────────────────────────────
    const graded = await gradeAllParallel(withMetadata, jobId);

    // ── Phase 5: Assemble ────────────────────────────────────────────────────
    gradedAnswers = assembleResults(skipped, graded);
  }

  // ── Aggregate ──────────────────────────────────────────────────────────────
  const { totalMarks, maxMarks, percentage, gradingStatus, flaggedQuestions } =
    aggregateResults(gradedAnswers);

  const processingTimeMs = Date.now() - startedAt;

  log.info('GRADING_PIPELINE_COMPLETE', {
    jobId, gradingStatus, totalMarks, maxMarks, percentage,
    flaggedCount: flaggedQuestions.length,
    processingTimeMs,
  });

  return {
    examId, subjectId, student, jobId,
    gradingStatus, totalMarks, maxMarks, percentage,
    flaggedQuestions, gradedAnswers,
    gradedAt:         new Date().toISOString(),
    processingTimeMs,
  };
}