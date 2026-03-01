# 🤖 AI Exam Evaluation System

An end-to-end scalable AI-powered exam copy evaluation system that automatically processes handwritten exam papers, performs OCR, segments answers, and evaluates them using Large Language Models (LLMs).

Built for high-scale environments (10,000+ copies/day).

---

## 🚀 Overview

This system enables automated evaluation of handwritten exam copies using:

- Google Cloud Storage (Signed URL uploads)
- Google Document AI (OCR)
- Google Pub/Sub (Event-driven processing)
- Dedicated Worker Server (Segmentation + LLM grading)
- Large Language Models (Rubric-based scoring)

The architecture is designed for performance, cost optimization, and reliability.

---
```
## 🏗 Architecture

Student Upload
↓
Google Cloud Storage
↓
Cloud Function (OCR via Document AI)
↓
Pub/Sub (text payload)
↓
Dedicated Worker
↓
LLM Grading
↓
Database
↓
Dashboard / Analytics
```


---

## ⚙️ Key Features

- ✅ Handwritten exam OCR using Document AI
- ✅ Event-driven processing with Pub/Sub
- ✅ Production-grade worker with retry & idempotency
- ✅ Rubric-based AI grading
- ✅ Scalable to 10,000+ copies per day
- ✅ Dead-letter queue support
- ✅ Cost-optimized LLM usage
- ✅ Secure signed URL uploads
- ✅ Modular microservice architecture

---

## 📦 Tech Stack

### Cloud Layer
- Google Cloud Storage
- Google Document AI
- Google Pub/Sub
- Cloud Functions (Gen2)

### Backend
- Node.js (ES Modules)
- Dedicated Worker Process
- Redis / Queue (optional for scaling)
- MongoDB / PostgreSQL (for grading storage)

### AI
- LLM (OpenAI / Gemini / Azure OpenAI)
- Structured JSON output grading
- Low-temperature deterministic scoring

---

## 🔐 Security Design

- Signed URL based secure uploads
- No direct file upload to backend
- Pub/Sub message validation
- Idempotent grading to prevent duplicate processing
- Dead-letter topic for failed jobs
- Service Account based authentication

---

## 💰 Cost Optimized Design

The system minimizes cost by:

- Sending only extracted OCR text (not full JSON) via Pub/Sub
- Avoiding redundant GCS downloads
- Using short structured LLM prompts
- Limiting token output
- Processing question-wise instead of full document grading

---

## 🧠 Grading Strategy

Each answer is evaluated using:

- Fixed question paper mapping
- Predefined sample solution (rubric)
- Strict evaluation rules
- JSON-based scoring output

Example LLM output:

```json
{
  "score": 4,
  "matched_points": ["Definition correct", "Example present"],
  "missing_points": ["Explanation incomplete"],
  "confidence": 0.87
}
```

🔁 Retry & Reliability

Pub/Sub automatic retries

Max delivery attempts configured

Dead-letter queue enabled

Retry only for temporary errors

Timeout protection for LLM calls

Idempotency check before grading

📊 Scalability

Designed to handle:

10,000 copies/day

100,000+ total evaluations

Parallel worker scaling

Flow control protection

Scaling options:

Horizontal worker scaling

Docker containerization

Kubernetes support

🗂 Project Structure
```

backend/
│
├── src/
│   ├── app.js
│   ├── workers/
│   │   └── ocrWorker.js
│   ├── services/
│   └── utils/
│
functions/
└── process-ocr/
├── index.js
└── package.json
```
🛠 Setup
1️⃣ Install Dependencies
```
npm install
```
2️⃣ Start API Server
```
npm run start
```
3️⃣ Start Worker
```
npm run worker
```
🌩 Deploy Cloud Function
```
gcloud functions deploy processOCR \
  --gen2 \
  --runtime=nodejs22 \
  --region=asia-south1 \
  --source=functions/process-ocr \
  --entry-point=processOCR \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=ai-exam-storage-470609-q7"
```

📈 Performance

Average processing time per copy:

OCR: 3–8 seconds

Pub/Sub delivery: 1–3 seconds

LLM grading: 5–15 seconds (depending on model)

Total: ~10–25 seconds end-to-end

🧪 Future Improvements

Diagram and math formula recognition

RAG-based grading using syllabus material

AI confidence-based human review routing

Real-time teacher dashboard

Per-topic weakness analytics

📌 Use Cases

Schools

Universities

Competitive exam institutes

EdTech platforms

Mock test evaluation systems

👨‍💻 Author

Sagar Saini

Software Developer | AI System Builder
