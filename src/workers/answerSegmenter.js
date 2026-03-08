import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv from "dotenv";

dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model: "gemini-3.1-flash-lite-preview",
});

/* ─── Retry config ────────────────────────────────────────────────────────── */
const RETRY_CONFIG = {
  maxAttempts: 3,
  baseDelayMs: 1000,
  maxDelayMs: 10000,
};

/* ─── Timeout protection ─────────────────────────────────────────────────── */
function withTimeout(promise, ms, label = "operation") {
  const timeout = new Promise((_, reject) =>
    setTimeout(
      () => reject(Object.assign(new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: "ETIMEDOUT" })),
      ms
    )
  );
  return Promise.race([promise, timeout]);
}

/* ─── Retryable error detection ──────────────────────────────────────────── */
function isRetryable(err) {
  if (!err) return false;
  if (["ETIMEDOUT", "ECONNRESET", "EAI_AGAIN"].includes(err.code)) return true;
  if (err.message?.startsWith("Timeout:")) return true;
  const status = err.status ?? err.response?.status;
  if (status === 429) return true;
  if (status >= 500)  return true;
  return false;
}

/* ─── Exponential backoff ────────────────────────────────────────────────── */
function getBackoffDelay(attempt) {
  const jitter = Math.random() * 500;
  return Math.min(
    RETRY_CONFIG.baseDelayMs * Math.pow(2, attempt) + jitter,
    RETRY_CONFIG.maxDelayMs
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/* ─── JSON extractor ─────────────────────────────────────────────────────── */
function extractJSON(text) {
  if (!text || typeof text !== "string") return null;

  try {
    return JSON.parse(text.trim());
  } catch {
    try {
      const start = text.indexOf("{");
      const end   = text.lastIndexOf("}");
      if (start === -1 || end === -1 || end <= start) return null;
      return JSON.parse(text.substring(start, end + 1));
    } catch (err) {
      console.error("JSON parse error:", err.message);
      return null;
    }
  }
}

/* ─── Validate parsed structure ──────────────────────────────────────────── */
function isValidSegmentation(parsed) {
  return (
    parsed &&
    typeof parsed === "object" &&
    Array.isArray(parsed.questions) &&
    parsed.questions.every(
      (q) =>
        typeof q.questionNumber === "string" &&
        typeof q.answer         === "string"
    )
  );
}

/* ─── Core LLM call (single attempt) ─────────────────────────────────────── */
async function callLLM(text) {
  const prompt = `
You are an AI system that segments OCR text of handwritten exam answer sheets.

Your task is ONLY to split the OCR text into answers.

DO NOT change any text.

--------------------------------

RULES FOR QUESTION DETECTION

1. Detect question numbers exactly as written.

Examples:
(क) (ख) (ग) (घ) (ङ)
(i) (ii) (iii) (iv) (v)
1(i) 2(ii)
Q1 Q2
प्रश्नोत्तर सं० - 2

2. If a section header appears like:

"प्रश्नोत्तर सं० - 2"

and below it subquestions appear like:

(i) (ii) (iii) (iv) (v)

then the correct question numbers must be:

2(i)
2(ii)
2(iii)
2(iv)
2(v)

3. Each answer begins AFTER the question number and continues until the next question number appears.

4. Do NOT modify OCR text.

5. Do NOT fix spelling.

6. Do NOT correct mathematics.

7. Preserve the text exactly as written.

8. If OCR text looks incorrect, keep it unchanged.

OUTPUT FORMAT (STRICT JSON):

{
  "questions": [
    {
      "questionNumber": "...",
      "answer": "..."
    }
  ]
}

IMPORTANT:
Return ONLY JSON.
No explanation.
No markdown.
No additional text.

OCR TEXT:
${text}
`;

  const result = await model.generateContent(prompt);
  const raw = result.response.candidates?.[0]?.content?.parts?.[0]?.text ?? "";

  if (!raw.trim()) {
    const err = new Error("LLM returned empty response");
    err.nonRetryable = true;
    throw err;
  }

  return raw;
}

/* ─── Main export with retry + timeout ───────────────────────────────────── */
export async function segmentAnswersWithLLM(text) {
  console.log("\n=========== ANSWER SEGMENTATION STARTED ===========");
  console.log(`timestamp  : ${new Date().toISOString()}`);
  console.log(`inputLength: ${text?.length ?? 0} chars`);
  console.log("====================================================\n");

  if (!text || !text.trim()) {
    console.warn("segmentAnswersWithLLM: empty text provided");
    return { questions: [] };
  }

  let lastError = null;

  for (let attempt = 0; attempt < RETRY_CONFIG.maxAttempts; attempt++) {
    try {
      if (attempt > 0) {
        const delay = getBackoffDelay(attempt - 1);
        console.warn(
          `segmentAnswersWithLLM: retry attempt ${attempt}/${RETRY_CONFIG.maxAttempts - 1} after ${Math.round(delay)}ms — ${lastError?.message}`
        );
        await sleep(delay);
      }

      console.log(`segmentAnswersWithLLM: calling Gemini (attempt ${attempt + 1})...`);

      const raw = await withTimeout(
        callLLM(text),
        45_000,
        "answer segmentation"
      );

      const parsed = extractJSON(raw);

      if (!parsed) {
        console.warn(`Attempt ${attempt + 1}: LLM returned non-JSON:\n${raw.slice(0, 200)}`);
        lastError = new Error("LLM returned non-JSON");
        lastError.nonRetryable = true;
        break;
      }

      if (!isValidSegmentation(parsed)) {
        console.warn(
          `Attempt ${attempt + 1}: JSON structure invalid:`,
          JSON.stringify(parsed).slice(0, 200)
        );
        lastError = new Error("Invalid segmentation structure");
        lastError.nonRetryable = true;
        break;
      }

      if (attempt > 0) {
        console.log(`segmentAnswersWithLLM: succeeded on attempt ${attempt + 1}`);
      }

      console.log("\n=========== ANSWER SEGMENTATION COMPLETE ===========");
      console.log(`questionsFound: ${parsed.questions.length}`);
      console.log("=====================================================\n");

      return parsed;

    } catch (err) {
      lastError = err;

      if (err.nonRetryable || !isRetryable(err)) {
        console.error(
          `segmentAnswersWithLLM: non-retryable error on attempt ${attempt + 1}:`,
          err.message
        );
        break;
      }

      console.warn(
        `segmentAnswersWithLLM: retryable error on attempt ${attempt + 1}:`,
        err.message,
        err.status ? `(HTTP ${err.status})` : ""
      );
    }
  }

  console.error(
    "segmentAnswersWithLLM: all attempts failed, returning empty. Last error:",
    lastError?.message
  );
  return { questions: [] };
}