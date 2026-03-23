/**
 * gradingUtils.js
 * Pre-grading checks, answer type detection, flag builder.
 * Pure functions — no API calls, no side effects.
 */

// ─── Constants ────────────────────────────────────────────────────────────────

const MIN_ANSWER_LENGTH = 10;   // chars se kam = blank treat karein

// Keywords jo indicate karte hain visual content
const VISUAL_KEYWORDS = [
  'diagram', 'draw', 'label', 'map', 'figure', 'sketch',
  'illustrate', 'mark', 'shade', 'plot', 'chart', 'table',
  'आरेख', 'चित्र', 'मानचित्र', 'नक्शा',
];

// Math indicators
const MATH_KEYWORDS = [
  '=', '+', '-', '×', '÷', '/', 'π', '√', '^',
  'sin', 'cos', 'tan', 'log', 'formula', 'equation',
  'calculate', 'find', 'prove', 'solve',
  'm/s', 'km/h', 'kg', 'joule', 'watt', 'ohm', 'newton',
];

// Vector distance threshold — isse zyada = low confidence match
const LOW_MATCH_THRESHOLD = 0.75;

// ─── Answer type detection ────────────────────────────────────────────────────

/**
 * Answer text aur question text se answer type detect karo.
 * @returns {"text"|"visual"|"formula"|"mixed"}
 */
export function detectAnswerType(studentAnswer = '', question = '') {
  const combined = `${question} ${studentAnswer}`.toLowerCase();

  const hasVisual = VISUAL_KEYWORDS.some(k => combined.includes(k.toLowerCase()));
  const hasMath   = MATH_KEYWORDS.some(k => studentAnswer.includes(k));

  if (hasVisual && hasMath) return 'mixed';
  if (hasVisual)             return 'visual';
  if (hasMath)               return 'formula';
  return 'text';
}

// ─── Pre-grading checks ───────────────────────────────────────────────────────

/**
 * Grading se pehle answer check karo.
 * Agar grading skip karni hai toh reason return karo.
 *
 * @returns {{ skip: boolean, skipReason: string|null, marksAwarded: number }}
 */
export function preGradingCheck(studentAnswer = '') {
  const trimmed = (studentAnswer ?? '').trim();

  // Bilkul blank
  if (!trimmed) {
    return { skip: true, skipReason: 'blank_answer', marksAwarded: 0 };
  }

  // Sirf question number ya ek word
  if (trimmed.length < MIN_ANSWER_LENGTH) {
    return { skip: true, skipReason: 'answer_too_short', marksAwarded: 0 };
  }

  return { skip: false, skipReason: null, marksAwarded: null };
}

// ─── Flag builder ─────────────────────────────────────────────────────────────

/**
 * Grading result se flags generate karo.
 * Teacher review ke liye kaunse questions flag hone chahiye.
 *
 * @param {object} opts
 * @returns {string[]}  flag strings array
 */
export function buildFlags({
  matchDistance,
  marksAwarded,
  studentAnswer,
  answerType,
  geminiConfidence,
  skipReason,
  isUnmatched,
}) {
  const flags = [];

  if (isUnmatched) {
    flags.push('unmatched_question');
    return flags;   // agar match hi nahi hua toh baaki flags irrelevant
  }

  if (skipReason) {
    flags.push(skipReason);
    return flags;
  }

  if (matchDistance > LOW_MATCH_THRESHOLD) {
    flags.push('low_match_confidence');
  }

  if (geminiConfidence === 'low') {
    flags.push('ai_low_confidence');
  }

  // Zero marks mila lekin answer likha tha — teacher verify kare
  if (marksAwarded === 0 && (studentAnswer ?? '').trim().length > 50) {
    flags.push('zero_marks_with_content');
  }

  if (answerType === 'visual' || answerType === 'mixed') {
    flags.push('visual_answer');
  }

  return flags;
}

// ─── Result aggregator ────────────────────────────────────────────────────────

/**
 * Saare graded answers se total marks aur status calculate karo.
 *
 * @param {Array} gradedAnswers
 * @returns {{ totalMarks, maxMarks, percentage, gradingStatus, flaggedQuestions }}
 */
export function aggregateResults(gradedAnswers) {
  let totalMarks = 0;
  let maxMarks   = 0;
  const flaggedQuestions = [];

  for (const ans of gradedAnswers) {
    // Teacher override ko priority do
    const effectiveMarks = ans.marksOverride ?? ans.marksAwarded ?? 0;
    totalMarks += effectiveMarks;
    maxMarks   += ans.maxMarks ?? 0;

    if (ans.flags?.length > 0) {
      flaggedQuestions.push(ans.questionNumber);
    }
  }

  const percentage = maxMarks > 0
    ? Math.round((totalMarks / maxMarks) * 100)
    : 0;

  // gradingStatus decide karo
  const unmatched = gradedAnswers.filter(a => a.flags?.includes('unmatched_question'));
  const hasFlags  = flaggedQuestions.length > 0;

  let gradingStatus;
  if (unmatched.length > 0) {
    gradingStatus = 'partial';        // kuch questions match nahi hue
  } else if (hasFlags) {
    gradingStatus = 'needs_review';   // teacher ko check karna chahiye
  } else {
    gradingStatus = 'completed';      // sab clean
  }

  return { totalMarks, maxMarks, percentage, gradingStatus, flaggedQuestions };
}