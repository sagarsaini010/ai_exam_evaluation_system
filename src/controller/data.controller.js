import { Storage } from "@google-cloud/storage";

// Uses Application Default Credentials (ADC) — no key file needed in production.
// Locally: run `gcloud auth application-default login`
// On GCP (Cloud Run / GKE): workload identity is picked up automatically.
const storage = new Storage({
   keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
});

// ─── Config ───────────────────────────────────────────────────────────────────
const CENTRAL_BUCKET       = process.env.CENTRAL_BUCKET || "ai-exam-storage-470609-q7";
const SIGNED_URL_EXPIRY_MS = 60 * 60 * 1000;          // 1 hour
const MAX_FILE_SIZE        = 10 * 1024 * 1024;         // 10 MB

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

/**
 * Uploads a single file buffer to GCS and returns a signed read URL.
 * Custom metadata is stored on the object so the OCR worker can identify
 * which student / class / exam the file belongs to without parsing the path.
 */
async function uploadFileToGCS({ buffer, mimetype, filePath, meta }) {
  const bucket  = storage.bucket(CENTRAL_BUCKET);
  const gcsFile = bucket.file(filePath);

  await new Promise((resolve, reject) => {
    const stream = gcsFile.createWriteStream({
      resumable: false,
      contentType: mimetype,
      metadata: {
        contentDisposition: "inline",
        // Custom metadata — readable by OCR / AI workers via object.getMetadata()
        metadata: {
          schoolName: meta.schoolName,
          branchId:   meta.branchId || "default",
          classId:    meta.classId,
          sectionId:  meta.sectionId,
          studentId:  meta.studentId,
        },
      },
    });
    stream.on("error", reject);
    stream.on("finish", resolve);
    stream.end(buffer);
  });

  const [signedUrl] = await gcsFile.getSignedUrl({
    version: "v4",
    action: "read",
    expires: Date.now() + SIGNED_URL_EXPIRY_MS,
  });

  return { filePath, signedUrl };
}

// ─── Controllers ─────────────────────────────────────────────────────────────

/**
 * POST /api/v1/upload
 * Accepts multipart files + metadata in req.body.
 * Stores each file under a structured path in the central bucket.
 */
export async function processData(req, res) {
  try {
    if (!req.files?.length) {
      return res.status(400).json({ success: false, message: "No files uploaded" });
    }

    const { schoolName, branchId, classId, sectionId, studentId } = req.body;

    if (!schoolName || !classId || !sectionId || !studentId) {
      return res.status(400).json({
        success: false,
        message: "schoolName, classId, sectionId, and studentId are required in form body",
      });
    }

    // Validate every file before touching GCS
    for (const file of req.files) {
      if (!ALLOWED_TYPES.includes(file.mimetype)) {
        return res.status(400).json({
          success: false,
          message: `Invalid file type: ${file.originalname}. Allowed: jpg, jpeg, png, pdf`,
        });
      }
      if (file.size > MAX_FILE_SIZE) {
        return res.status(400).json({
          success: false,
          message: `File too large: ${file.originalname}. Maximum size is 10 MB`,
        });
      }
    }

    const meta = { schoolName, branchId, classId, sectionId, studentId };

    const uploadPromises = req.files.map((file) => {
      const filePath = buildFilePath({
        schoolName,
        branchId,
        classId,
        sectionId,
        studentId,
        fileName: file.originalname,
      });

      return uploadFileToGCS({
        buffer: file.buffer,
        mimetype: file.mimetype,
        filePath,
        meta,
      }).then((result) => ({ originalName: file.originalname, ...result }));
    });

    const settled = await Promise.allSettled(uploadPromises);

    const uploaded = settled
      .filter((r) => r.status === "fulfilled")
      .map((r) => r.value);

    const errors = settled
      .filter((r) => r.status === "rejected")
      .map((r) => r.reason?.message || "Unknown error");

    return res.status(uploaded.length ? 200 : 500).json({
      success: uploaded.length > 0,
      message: `Uploaded ${uploaded.length} of ${req.files.length} files`,
      uploaded,
      ...(errors.length && { errors }),
    });

  } catch (error) {
    console.error("Upload error:", error);
    return res.status(500).json({ success: false, message: "Server error during upload" });
  }
}

/**
 * POST /api/v1/generate-upload-url
 * Returns a signed PUT URL so the frontend can upload directly to GCS.
 * Useful for large files — avoids routing the file through your server.
 */
export async function generateUploadUrl(req, res) {
  try {
    const { fileName, contentType, schoolName, branchId, classId, sectionId, studentId } =
      req.body;

    if (!fileName || !contentType || !schoolName || !classId || !sectionId || !studentId) {
      return res.status(400).json({
        success: false,
        message: "fileName, contentType, schoolName, classId, sectionId, and studentId are required",
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

    const [uploadUrl] = await file.getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 15 * 60 * 1000, // 15 minutes to complete the upload
      contentType,
    });

    return res.json({
      success: true,
      uploadUrl,
      bucketName: CENTRAL_BUCKET,
      filePath,
      expiresIn: "15 minutes",
    });

  } catch (error) {
    console.error("Generate upload URL error:", error);
    return res.status(500).json({ success: false, message: error.message });
  }
}