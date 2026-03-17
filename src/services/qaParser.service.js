import { GoogleGenerativeAI } from '@google/generative-ai';
import dotenv from "dotenv";

dotenv.config();
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model:            'gemini-3.1-flash-lite-preview',
  generationConfig: { temperature: 0 },
});

const RETRY_CONFIG = { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10000 };
const MAX_TEXT_LENGTH = 50_000;

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

function isRetryable(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:')) return true;
  const status = err.status ?? err.response?.status;
  if (status === 429 || status >= 500) return true;
  return false;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

const SYSTEM_PROMPT = `You are an exam paper parser for Indian school teachers.
You will receive OCR text extracted from a typed Q&A answer-key PDF.

Your job: extract every question and its model answer into a JSON array.

RULES:
- Return ONLY a valid JSON array. No explanation, no markdown, no extra text.
- Each object must have exactly these fields:
    questionNo   : integer (sequential, starting from 1)
    section      : string or null  (e.g. "Section A" — null if no sections)
    question     : string (full question text)
    modelAnswer  : string (full model answer — do NOT summarise)
    maxMarks     : integer or null (extract if written, else null)
- Recognise all common numbering styles:
    "Q1.", "Q.1", "Question 1", "1.", "1)", "(1)"
- Recognise section headers:
    "Section A", "Section B", "Part I", "Part II", "खण्ड अ", "खण्ड ब"
- Handle mixed Hindi and English text — preserve both scripts exactly.
- Marks may appear as: "[5]", "(5 marks)", "Marks: 5", "5 अंक" — extract the integer.
- If marks are nowhere in the text, set maxMarks to null.
- If you find 0 questions, return an empty array [].

OUTPUT FORMAT:
[{ "questionNo": 1, "section": null, "question": "...", "modelAnswer": "...", "maxMarks": 5 }]`;

async function callGemini(ocrText) {
  const result = await model.generateContent(
    `${SYSTEM_PROMPT}\n\nPDF TEXT:\n${ocrText}\n\nReturn ONLY the JSON array starting with [`
  );

  const raw = result.response.candidates?.[0]?.content?.parts?.[0]?.text
           ?? result.response.text?.()
           ?? '';

  if (!raw.trim()) {
    throw Object.assign(new Error('Gemini returned empty response'), { nonRetryable: true });
  }
  return raw;
}

export async function parseQAFromText(ocrText) {
  const text = ocrText.length > MAX_TEXT_LENGTH
    ? ocrText.slice(0, MAX_TEXT_LENGTH)
    : ocrText;

  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = Math.min(
          RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 500,
          RETRY_CONFIG.maxDelayMs
        );
        console.warn(JSON.stringify({
          severity: 'WARNING', event: 'QA_PARSER_RETRY',
          attempt, error: lastError?.message,
        }));
        await sleep(delay);
      }

      const raw    = await withTimeout(callGemini(text), 60_000, 'Gemini QA parse');
      const cleaned = raw.replace(/```json|```/gi, '').trim();

      let parsed;
      try {
        parsed = JSON.parse(cleaned);
      } catch {
        const start = cleaned.indexOf('[');
        const end   = cleaned.lastIndexOf(']');
        if (start !== -1 && end > start) {
          parsed = JSON.parse(cleaned.slice(start, end + 1));
        } else {
          throw Object.assign(new Error('Gemini returned non-JSON'), { nonRetryable: true });
        }
      }

      if (!Array.isArray(parsed)) {
        throw Object.assign(new Error('Response is not an array'), { nonRetryable: true });
      }

      // Normalise every field
      const questions = parsed.map((q, i) => ({
        questionNo:  typeof q.questionNo === 'number' ? q.questionNo : i + 1,
        section:     q.section     ? String(q.section).trim()     : null,
        question:    q.question    ? String(q.question).trim()    : '',
        modelAnswer: q.modelAnswer ? String(q.modelAnswer).trim() : '',
        maxMarks:    typeof q.maxMarks === 'number' ? q.maxMarks  : null,
      }));

      console.log(JSON.stringify({
        severity: 'INFO', event: 'QA_PARSER_SUCCESS',
        questionsFound: questions.length,
      }));

      return { questions, parseError: false };

    } catch (err) {
      lastError = err;
      if (err.nonRetryable || !isRetryable(err)) {
        console.error(JSON.stringify({
          severity: 'ERROR', event: 'QA_PARSER_NON_RETRYABLE',
          attempt: attempt + 1, error: err.message,
        }));
        break;
      }
      console.warn(JSON.stringify({
        severity: 'WARNING', event: 'QA_PARSER_RETRYABLE',
        attempt: attempt + 1, error: err.message,
      }));
    }
  }

  console.error(JSON.stringify({
    severity: 'ERROR', event: 'QA_PARSER_ALL_ATTEMPTS_FAILED',
    error: lastError?.message,
  }));
  return { questions: [], parseError: true, rawError: lastError?.message };
}