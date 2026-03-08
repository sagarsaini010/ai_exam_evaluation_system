import { GoogleGenerativeAI } from "@google/generative-ai";
import dotenv    from 'dotenv';

dotenv.config();

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model: "gemini-3-flash-preview"
});

export async function correctOCRText(ocrText) {

  console.log("\n=========== OCR BEFORE LLM CORRECTION ===========\n");
  console.log(ocrText);
  console.log("\n===============================================\n");

  const prompt = `
You are cleaning OCR text from a handwritten exam sheet.

Your task is ONLY to fix obvious OCR noise.

Allowed fixes:
- join broken words
- remove random OCR characters
- fix spacing
- math symbols (π, √, sin, cos etc)
- fix obvious spelling errors

STRICT RULES:

1. DO NOT change numbers.
2. DO NOT change mathematical expressions.
3. DO NOT guess missing characters.
4. If uncertain, keep the original text.

Preserve all Hindi text exactly.

Return ONLY cleaned text.

OCR TEXT:
${ocrText}

`;

  const result = await model.generateContent(prompt);

  const correctedText =
    result.response.candidates?.[0]?.content?.parts?.[0]?.text || ocrText;

  console.log("\n=========== OCR AFTER LLM CORRECTION ===========\n");
  console.log(correctedText);
  console.log("\n===============================================\n");

  return correctedText.trim();
}