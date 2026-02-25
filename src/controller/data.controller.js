import { Storage } from "@google-cloud/storage";

const storage = new Storage({
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
});

const defaultBucketName = "copy-checking-bucket-asia";

function sanitizeSegment(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function buildBucketName({ schoolName, branchId }) {
  const school = sanitizeSegment(schoolName);
  const branch = sanitizeSegment(branchId || "default");

  // GCS bucket max length is 63 chars.
  const raw = `school-${school}-${branch}`;
  return raw.slice(0, 63);
}

function buildStudentPath({ classId, sectionId, studentId, fileName }) {
  const safeFileName = `${Date.now()}-${fileName.replace(/[^a-zA-Z0-9.-]/g, "_")}`;
  return [
    "classes",
    sanitizeSegment(classId),
    "sections",
    sanitizeSegment(sectionId),
    "students",
    sanitizeSegment(studentId),
    safeFileName,
  ].join("/");
}

async function ensureBucketExists(bucketName) {
  const bucket = storage.bucket(bucketName);
  const [exists] = await bucket.exists();

  if (!exists) {
    await storage.createBucket(bucketName, {
      location: process.env.GCS_BUCKET_LOCATION || "asia-south1",
      storageClass: process.env.GCS_BUCKET_STORAGE_CLASS || "STANDARD",
    });
  }

  return bucket;
}

export async function processData(req, res) {
  try {
    if (!req.files?.length) {
      return res.status(400).json({ success: false, message: "No files uploaded" });
    }

    const bucket = storage.bucket(defaultBucketName);

    const uploadPromises = req.files.map(async (file) => {
      const safeName = `${Date.now()}-${file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_')}`;
      const gcsFile = bucket.file(`uploads/${safeName}`);

      const blobStream = gcsFile.createWriteStream({
        resumable: false,
        contentType: file.mimetype,
        metadata: {
          contentDisposition: 'inline', // browser में preview के लिए
          // custom metadata अगर चाहिए: examId, etc.
        }
      });

      return new Promise((resolve, reject) => {
        blobStream.on("error", reject);
        blobStream.on("finish", async () => {
          try {
            const [signedUrl] = await gcsFile.getSignedUrl({
              version: "v4",
              action: "read",
              expires: Date.now() + 60 * 60 * 1000, // 1 hour — evaluation के लिए काफी
            });
            resolve({ originalName: file.originalname, gcsPath: gcsFile.name, signedUrl });
          } catch (err) {
            reject(err);
          }
        });
        blobStream.end(file.buffer);
      });
    });

    const settled = await Promise.allSettled(uploadPromises);
    const uploaded = settled
      .filter(r => r.status === "fulfilled")
      .map(r => r.value);

    const errors = settled
      .filter(r => r.status === "rejected")
      .map(r => r.reason?.message || "Unknown error");

    res.status(uploaded.length ? 200 : 500).json({
      success: uploaded.length > 0,
      message: `Uploaded ${uploaded.length} of ${req.files.length} files`,
      uploaded,
      errors: errors.length ? errors : undefined
    });

  } catch (error) {
    console.error("Upload error:", error);
    res.status(500).json({ success: false, message: "Server error during upload" });
  }
}

export async function generateUploadUrl(req, res) {
  try {
    const {
      fileName,
      contentType,
      schoolName,
      branchId,
      classId,
      sectionId,
      studentId,
      metadata = {},
    } = req.body;

    if (!fileName || !contentType || !schoolName || !classId || !sectionId || !studentId) {
      return res.status(400).json({
        success: false,
        message:
          "fileName, contentType, schoolName, classId, sectionId and studentId are required",
      });
    }

    const bucketName = buildBucketName({ schoolName, branchId });
    const bucket = await ensureBucketExists(bucketName);
    const filePath = buildStudentPath({ classId, sectionId, studentId, fileName });
    const file = bucket.file(filePath);

    const [signedUrl] = await file.getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 15 * 60 * 1000,
      contentType,
      extensionHeaders: {
        "x-goog-meta-school-name": schoolName,
        "x-goog-meta-branch-id": String(branchId || ""),
        "x-goog-meta-class-id": String(classId),
        "x-goog-meta-section-id": String(sectionId),
        "x-goog-meta-student-id": String(studentId),
      },
    });

    res.json({
      success: true,
      uploadUrl: signedUrl,
      bucketName,
      filePath,
      expiresIn: "15 minutes",
      metadata,
    });

  } catch (error) {
    console.error("Generate upload URL error:", error);
    res.status(500).json({ success: false, message: error.message });
  }
}
