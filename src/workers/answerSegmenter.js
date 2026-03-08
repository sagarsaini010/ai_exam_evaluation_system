import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv    from 'dotenv';

dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model: "gemini-3-flash-preview"
});

export async function segmentAnswersWithLLM(text) {

const prompt = `
You are an AI system that extracts student answers from OCR text of handwritten exam sheets.

Your task is ONLY to segment the text into answers.

STRICT RULES:

1. Detect question numbers EXACTLY as written in the text.

Examples:
(क) (ख) (ग) (घ) (ङ)
(i) (ii) (iii) (iv) (v)
1(i) 2(ii) 3(iii)
Q1 Q2 Q3
प्रश्नोत्तर सं०

2. Each answer starts AFTER its question number and continues until the next question number appears.

3. DO NOT modify the text.
4. DO NOT correct mathematics.
5. DO NOT guess missing characters.
6. DO NOT invent answers.
7. Preserve all OCR text exactly as written.

8. If OCR text looks incorrect, keep it exactly the same.

OUTPUT FORMAT (STRICT JSON):

{
 "questions":[
   {
     "questionNumber":"...",
     "answer":"..."
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

  const raw =
    result.response.candidates?.[0]?.content?.parts?.[0]?.text || "";
 console.log("raw ==", raw)
  const parsed = extractJSON(raw);

  if (!parsed) {
    console.log("LLM returned non-JSON");
    return { questions: [] };
  }

  return parsed;
}

function extractJSON(text) {

  try {

    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");

    if (start === -1 || end === -1) {
      return null;
    }

    const jsonString = text.substring(start, end + 1);

    return JSON.parse(jsonString);

  } catch (err) {

    console.log("JSON parse error:", err.message);
    return null;

  }

}