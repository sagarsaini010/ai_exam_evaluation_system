import { Storage } from "@google-cloud/storage";

const storage = new Storage({
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
});

const bucketName = "copy-checking-bucket-asia";

export async function processData(req, res) {
  try {
    if (!req.files?.length) {
      return res.status(400).json({ success: false, message: "No files uploaded" });
    }

    const bucket = storage.bucket(bucketName);
    const results = [];

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
    const { fileName, contentType, metadata = {} } = req.body; // metadata: { examId, studentId }
    console.log("Generating upload URL for:", { fileName, contentType, metadata });
    if (!fileName || !contentType) {
      return res.status(400).json({ success: false, message: "fileName and contentType required" });
    }
    
    const safeName = `${Date.now()}-${fileName.replace(/[^a-zA-Z0-9.-]/g, '_')}`;
    const file = storage.bucket(bucketName).file(`uploads/${safeName}`);

    const [signedUrl] = await file.getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 15 * 60 * 1000, // 15 min — enough for upload
      contentType,
      // optional: virtualHostedStyle: true (if custom domain)
    });

    res.json({
      success: true,
      uploadUrl: signedUrl,
      filePath: file.name,
      expiresIn: "15 minutes"
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ success: false, message: error.message });
  }
}