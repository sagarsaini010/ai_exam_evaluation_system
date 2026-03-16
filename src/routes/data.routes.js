import express from "express";
import{ generateUploadUrl, giveStatus} from "../controller/data.controller.js";


const router = express.Router();

// multer middleware used here
router.post("/generate-upload-url", generateUploadUrl);

router.get('/status/:jobId', giveStatus);

export default router;