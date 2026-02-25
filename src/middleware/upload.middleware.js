import multer from "multer";

const fileFilter = (req, file, cb) => {
  const allowed = [
    "application/pdf",
    "image/jpeg",
    "image/png",          // add PNG अगर handwritten photos में common हो
    "image/webp"          // modern phones
  ];
  if (allowed.includes(file.mimetype)) {
    cb(null, true);
  } else {
    cb(new Error("Only PDF, JPEG, PNG, WEBP allowed"), false);
  }
};

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB — answer sheets के लिए safe (scanned PDFs ~5-12MB)
  fileFilter,
});

export default upload;

// gcloud functions logs read processOCR --region asia-south1 --limit 100


// gcloud ai processors create --location=asia-south1 --type=OCR_PROCESSOR --display-name=ExamOCR

