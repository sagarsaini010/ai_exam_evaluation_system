import express from "express";
import{ processData, generateUploadUrl} from "../controller/data.controller.js";
import upload from "../middleware/upload.middleware.js";

const router = express.Router();

// multer middleware used here
router.post("/upload", upload.array("files", 10), processData);
router.post("/generate-upload-url", generateUploadUrl);
export default router;