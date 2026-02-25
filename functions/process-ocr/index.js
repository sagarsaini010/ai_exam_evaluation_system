const path = require('path');
const functions = require('@google-cloud/functions-framework');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { Storage } = require('@google-cloud/storage');

const projectId = process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7';
const location = process.env.DOCUMENT_AI_LOCATION || 'asia-south1';
const processorId = process.env.DOCUMENT_AI_PROCESSOR_ID || 'f9b5a9f31d819f11';

// FIX 1: Initialize with regional endpoint
const docAIClient = new DocumentProcessorServiceClient({
  apiEndpoint: `${location}-documentai.googleapis.com`,
});
const storage = new Storage();

function buildOcrOutputPath(inputPath) {
  const dir = path.posix.dirname(inputPath);
  const base = path.posix.basename(inputPath, path.posix.extname(inputPath));
  return path.posix.join(dir === '.' ? '' : dir, `${base}-ocr.json`);
}

function shouldSkipFile(filePath, metadata = {}) {
  return !filePath || filePath.endsWith('-ocr.json') || metadata.ocrGenerated === 'true';
}

functions.cloudEvent('processOCR', async (cloudEvent) => {
  const fileData = cloudEvent.data || {};
  const bucketName = fileData.bucket;
  const filePath = fileData.name;

  if (!bucketName || !filePath) return;
  if (shouldSkipFile(filePath, fileData.metadata)) return;

  console.log(`Processing upload: gs://${bucketName}/${filePath}`);

  try {
    const processorName = `projects/${projectId}/locations/${location}/processors/${processorId}`;
    const extension = filePath.split('.').pop().toLowerCase();
    
    const mimeTypes = { pdf: 'application/pdf', jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png' };
    const mimeType = mimeTypes[extension];

    if (!mimeType) throw new Error(`Unsupported file type: ${extension}`);

    // FIX 2: Download file to memory (processDocument requires raw bytes)
    const [fileBuffer] = await storage.bucket(bucketName).file(filePath).download();

    // FIX 3: Reformat request to use rawDocument
    const request = {
      name: processorName,
      rawDocument: {
        content: fileBuffer,
        mimeType: mimeType,
      },
    };

    const [result] = await docAIClient.processDocument(request);
    const document = result.document;

    if (!document) throw new Error('Document AI returned no document.');

    const outputPath = buildOcrOutputPath(filePath);
    await storage.bucket(bucketName).file(outputPath).save(
      JSON.stringify({
        sourceFile: filePath,
        bucket: bucketName,
        text: document.text || '',
        document,
        generatedAt: new Date().toISOString(),
      }, null, 2),
      {
        contentType: 'application/json',
        metadata: { metadata: { ocrGenerated: 'true', sourceFile: filePath } },
      }
    );

    console.log(`OCR output saved: gs://${bucketName}/${outputPath}`);
  } catch (error) {
    console.error(`OCR processing failed for gs://${bucketName}/${filePath}:`, error.message);
  }
});


// Example deployment:
// gcloud functions deploy processOCR --gen2 --runtime nodejs22 --region asia-south1 --source functions/process-ocr --entry-point processOCR --trigger-event-filters="type=google.cloud.storage.object.v1.finalized"


// gcloud functions deploy processOCR --gen2 --runtime nodejs22 --region asia-south1 --source functions/process-ocr --entry-point processOCR --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" --trigger-event-filters="bucket=ai-exam-storage-470609-q7"

