import { Storage } from "@google-cloud/storage";
import { Firestore } from '@google-cloud/firestore';
import { v4 as uuidv4 } from 'uuid';
// Uses Application Default Credentials (ADC) — no key file needed in production.
// Locally: run `gcloud auth application-default login`
// On GCP (Cloud Run / GKE): workload identity is picked up automatically.
const storage = new Storage({
   keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
});
const firestore = new Firestore({ projectId: process.env.PROJECT_ID });
// ─── Config ───────────────────────────────────────────────────────────────────
const CENTRAL_BUCKET       = process.env.CENTRAL_BUCKET || "ai-exam-storage-470609-q7";


const ALLOWED_TYPES = [
  "image/jpeg",
  "image/png",
  "application/pdf",
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sanitizeSegment(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Builds a structured GCS path:
 * {schoolName}/{branchId}/{classId}/{sectionId}/{studentId}/{timestamp}-{fileName}
 */
function buildFilePath({ schoolName, branchId, classId, sectionId, studentId, fileName }) {
  const safeFileName = `${Date.now()}-${fileName.replace(/[^a-zA-Z0-9.-]/g, "_")}`;
  return [
    sanitizeSegment(schoolName),
    sanitizeSegment(branchId || "default"),
    sanitizeSegment(classId),
    sanitizeSegment(sectionId),
    sanitizeSegment(studentId),
    safeFileName,
  ].join("/");
}

// ─── Controllers ─────────────────────────────────────────────────────────────

/**
 * POST /api/v1/generate-upload-url
 * Returns a signed PUT URL so the frontend can upload directly to GCS.
 * Useful for large files — avoids routing the file through your server.
 */
export async function generateUploadUrl(req, res) {
  try {
    const { fileName, contentType, schoolName, branchId, classId, sectionId, studentId, examId, subjectId} =
      req.body;
    const jobId = uuidv4();
    if (!fileName || !contentType || !schoolName || !classId || !sectionId || !studentId || !examId || !subjectId) {
      return res.status(400).json({
        success: false,
        message: "fileName, contentType, schoolName, classId, sectionId, examId, subjectId and studentId are required",
      });
    }

    if (!ALLOWED_TYPES.includes(contentType)) {
      return res.status(400).json({
        success: false,
        message: `Invalid file type: ${contentType}. Allowed: jpg, jpeg, png, pdf`,
      });
    }

    const filePath = buildFilePath({ schoolName, branchId, classId, sectionId, studentId, fileName });

    const bucket = storage.bucket(CENTRAL_BUCKET);
    const file = bucket.file(filePath);

     // ── Signed URL — metadata headers encode karo ─────────────────────────
    // x-goog-meta-* headers signed URL mein lock ho jaate hain
    // Frontend ko PUT request mein exactly yahi headers bhejne padte hain
    const [uploadUrl] = await file.getSignedUrl({
      version:     'v4',
      action:      'write',
      expires:     Date.now() + 15 * 60 * 1000,
      contentType,
      extensionHeaders: {
        'x-goog-meta-jobid':      jobId,
        'x-goog-meta-schoolname': schoolName || '',
        'x-goog-meta-studentid':  studentId  || '',
        'x-goog-meta-branchid':   branchId   || '',
        'x-goog-meta-classid':    classId    || '',
        'x-goog-meta-sectionid':  sectionId  || '',
        'x-goog-meta-examid':     examId    || '',   // ← add
        'x-goog-meta-subjectid':  subjectId || '',   // ← add
      },
    });
  // ── Step 3: Firestore mein job create karo ────────────────────────────
    await firestore.collection('exam_jobs').doc(jobId).set({
      jobId,
      status:    'pending',
      filePath,
      createdAt:    new Date().toISOString(),
      examId:       examId        || null,   // ← add
      subjectId:    subjectId     || null,   // ← add
      student: {
        schoolName: schoolName || null,
        branchId:   branchId   || null,
        classId:    classId    || null,
        sectionId:  sectionId  || null,
        studentId:  studentId  || null,
      },
    });

    return res.json({
      success:    true,
      uploadUrl,
      filePath,
      jobId,
      status:     'pending',
      expiresIn:  '15 minutes',
      requiredHeaders: {
        'Content-Type':           contentType,
        'x-goog-meta-jobid':      jobId,
        'x-goog-meta-schoolname': schoolName || '',
        'x-goog-meta-studentid':  studentId  || '',
        'x-goog-meta-branchid':   branchId   || '',
        'x-goog-meta-classid':    classId    || '',
        'x-goog-meta-sectionid':  sectionId  || '',
        'x-goog-meta-examid':     examId     || '',   // ← add
        'x-goog-meta-subjectid':  subjectId  || '',   // ← add
      },
    });

  } catch (error) {
    console.error("Generate upload URL error:", error);
    return res.status(500).json({ success: false, message: error.message });
  }
}

export async function giveStatus(req, res){

  const { jobId } = req.params;

  const doc = await firestore.collection('exam_jobs').doc(jobId).get();

  if (!doc.exists) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const job = doc.data();

  // Pending hai toh sirf status bhejo
  if (job.status === 'pending' || job.status === 'processing') {
    return res.json({ jobId, status: job.status });
  }

  // Completed hai toh result bhi bhejo
  if (job.status === 'completed') {
    return res.json({
      jobId,
      status:          'completed',
      questionsFound:  job.questionsFound,
      avgConfidence:   job.avgConfidence,
      segmentedAnswers: job.segmentedAnswers,
      processedAt:     job.processedAt,
      // Grading results — agar available hai
      gradingStatus:    job.gradingStatus    ?? null,
      totalMarks:       job.totalMarks       ?? null,
      maxMarks:         job.maxMarks         ?? null,
      percentage:       job.percentage       ?? null,
      flaggedQuestions: job.flaggedQuestions ?? [],
      gradedAnswers:    job.gradedAnswers    ?? [],
      gradedAt:         job.gradedAt        ?? null,
    });
  }
  // Grading chal raha hai
if (job.status === 'grading') {
  return res.json({
    jobId,
    status:  'grading',
    message: 'AI is grading your answers...',
  });
}

  // Failed
  return res.json({ jobId, status: 'failed', error: job.error });

}