const path = require('path');
const functions = require('@google-cloud/functions-framework');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { Storage } = require('@google-cloud/storage');

const projectId = process.env.GCP_PROJECT_ID || 'secure-brook-470609-q7';
const location = process.env.DOCUMENT_AI_LOCATION || 'asia-south1';
const processorId = process.env.DOCUMENT_AI_PROCESSOR_ID || 'f9b5a9f3d1d819f11';

const docAIClient = new DocumentProcessorServiceClient();
const storage = new Storage();

function buildOcrOutputPath(inputPath) {
  const dir = path.posix.dirname(inputPath);
  const base = path.posix.basename(inputPath, path.posix.extname(inputPath));
  const outputFile = `${base}-ocr.json`;

  return dir === '.' ? outputFile : path.posix.join(dir, outputFile);
}

function shouldSkipFile(filePath, metadata = {}) {
  if (!filePath) return true;

  if (filePath.endsWith('-ocr.json')) {
    return true;
  }

  if (metadata.ocrGenerated === 'true') {
    return true;
  }

  return false;
}

functions.cloudEvent('processOCR', async (cloudEvent) => {
  const fileData = cloudEvent.data || {};
  const bucketName = fileData.bucket;
  const filePath = fileData.name;

  if (!bucketName || !filePath) {
    console.log('Missing bucket or object path, ignoring event.');
    return;
  }

  if (shouldSkipFile(filePath, fileData.metadata)) {
    console.log(`Skipping file: gs://${bucketName}/${filePath}`);
    return;
  }

  console.log(`Processing upload: gs://${bucketName}/${filePath}`);

  try {
    const processorName = `projects/${projectId}/locations/${location}/processors/${processorId}`;

    const request = {
      name: processorName,
      gcsDocument: {
        gcsUri: `gs://${bucketName}/${filePath}`,
        mimeType: fileData.contentType || 'application/octet-stream',
      },
      processOptions: {
        ocrConfig: {
          enableNativePdfParsing: true,
          enableImageQualityScores: true,
        },
      },
    };

    const [result] = await docAIClient.processDocument(request);
    const document = result.document;

    if (!document) {
      throw new Error('Document AI returned no document.');
    }

    const outputPath = buildOcrOutputPath(filePath);
    const outputFile = storage.bucket(bucketName).file(outputPath);

    await outputFile.save(
      JSON.stringify(
        {
          sourceFile: filePath,
          bucket: bucketName,
          text: document.text || '',
          document,
          generatedAt: new Date().toISOString(),
        },
        null,
        2,
      ),
      {
        contentType: 'application/json',
        metadata: {
          metadata: {
            ocrGenerated: 'true',
            sourceFile: filePath,
          },
        },
      },
    );

    console.log(`OCR output saved: gs://${bucketName}/${outputPath}`);
  } catch (error) {
    console.error(`OCR processing failed for gs://${bucketName}/${filePath}:`, error.message);
    console.error(error.stack);
  }
});

// Example deployment:
// gcloud functions deploy processOCR --gen2 --runtime nodejs22 --region asia-south1 --source functions/process-ocr --entry-point processOCR --trigger-event-filters="type=google.cloud.storage.object.v1.finalized"
