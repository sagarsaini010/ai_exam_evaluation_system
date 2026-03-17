import express  from 'express';
import multer   from 'multer';
import { ingestPdf, confirmQA } from '../controller/qa.controller.js';

// Memory storage — buffer goes straight to Document AI, no disk write
const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 18 * 1024 * 1024 },   // 18 MB hard limit
  fileFilter: (_req, file, cb) => {
    file.mimetype === 'application/pdf'
      ? cb(null, true)
      : cb(Object.assign(new Error('Only PDF files are allowed'), { code: 'INVALID_TYPE' }));
  },
});

// Multer error handler — turns file-level errors into clean JSON responses
function handleUpload(req, res, next) {
  upload.single('pdf')(req, res, (err) => {
    if (!err) return next();
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ success: false, message: 'PDF exceeds 18 MB limit' });
    }
    if (err.code === 'INVALID_TYPE') {
      return res.status(400).json({ success: false, message: 'Only PDF files are allowed' });
    }
    return res.status(400).json({ success: false, message: err.message });
  });
}

const router = express.Router();

// Step 1 — teacher uploads PDF → get questions for preview
router.post('/ingest-pdf', handleUpload, ingestPdf);

// Step 2 — teacher confirms edited questions → embed + store
router.post('/confirm', confirmQA);

export default router;