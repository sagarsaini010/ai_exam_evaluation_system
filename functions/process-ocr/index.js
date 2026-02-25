const functions = require('@google-cloud/functions-framework');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { Storage } = require('@google-cloud/storage');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const projectId = 'secure-brook-470609-q7';
const location = 'asia-south1'; 
const processorId = 'f9b5a9f3d1d819f11';
const bucketName = 'copy-checking-bucket-asia';

const docAIClient = new DocumentProcessorServiceClient({

});

const storage = new Storage();
const bucket = storage.bucket(bucketName);

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');
console.log('Gemini API Key loaded from Secret Manager'); 
const model = genAI.getGenerativeModel({ model: 'gemini-3-flash-preview' }); 

functions.cloudEvent('processOCR', async (cloudEvent) => {
  const fileData = cloudEvent.data;

  if (!fileData || fileData.bucket !== bucketName) {
    console.log('Not our bucket, ignoring');
    return;
  }

  const filePath = fileData.name;
  if (!filePath.startsWith('uploads/')) {
    console.log(`Ignoring non-uploads file: ${filePath}`);
    return;
  }

  console.log(`Processing new upload: gs://${bucketName}/${filePath}`);

  try {
    // Step 1: Document AI OCR
    const name = `projects/${projectId}/locations/${location}/processors/${processorId}`;

    const request = {
  name,
  rawDocument: {  // single file के लिए rawDocument safe है (GCS URI के लिए भी काम करता है)
    content: null,  // GCS से process होने पर content null रहता है
    mimeType: file.contentType || 'image/jpeg',
    gcsSource: {
      uri: `gs://${file.bucket}/${file.name}`,
    },
  },
  processOptions: {
    ocrConfig: {
      enableNativePdfHandling: true,
      enableImageQualityAssessment: true,
      advancedOcrOptions: [
        'enable_handwriting_recognition',  // handwriting boost
        'enable_math_ocr',  // अगर math answers हों
      ],
    },
  },
};

console.log('Document AI Request:', JSON.stringify(request, null, 2));

    const [operation] = await docAIClient.processDocument(request);
    const [response] = await operation.promise();

    const document = response.document;
    if (!document) throw new Error('No document processed');

    // Step 2: Save raw OCR JSON to GCS
    const ocrFileName = `ocr-results/${Date.now()}-${path.basename(filePath, path.extname(filePath))}-ocr.json`;
    const ocrFile = bucket.file(ocrFileName);

    await ocrFile.save(JSON.stringify(response, null, 2), {
      contentType: 'application/json',
      metadata: { originalFile: filePath },
    });

    console.log(`OCR JSON saved: gs://${bucketName}/${ocrFileName}`);

    // Step 3: Extract & clean text
    let extractedText = document.text || '';
    extractedText = extractedText
      .replace(/\n+/g, '\n')
      .replace(/\s+/g, ' ')
      .trim();

    if (extractedText.length < 10) {
      console.warn('Very short text extracted, possible blank page');
    }

    // Step 4: Gemini Evaluation
    const prompt = `
You are an exam evaluator.
Evaluate this answer out of 5 marks.
Question: List and explain the Parts of Computer?
Answer:
${extractedText}

Give responses in JSON format ONLY with keys: 
"marks" (number 0-5), 
"feedback" (string), 
"reasoning" (string)
No extra text outside JSON.
    `;

    const result = await model.generateContent({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: {
        responseMimeType: 'application/json',
      },
    });

    const evalText = result.response.text();
    let evalJson;
    try {
      evalJson = JSON.parse(evalText);
    } catch (parseErr) {
      console.error('Gemini JSON parse failed:', parseErr);
      evalJson = { marks: 0, feedback: 'Evaluation failed', reasoning: evalText };
    }

    console.log('=== EVALUATION RESULT ===');
    console.log('File:', filePath);
    console.log('Extracted Text:', extractedText.substring(0, 200) + '...');
    console.log('Marks:', evalJson.marks);
    console.log('Feedback:', evalJson.feedback);
    console.log('Reasoning:', evalJson.reasoning);

    // Optional: Save evaluation also to GCS
    const evalFileName = `ocr-results/${Date.now()}-${path.basename(filePath, path.extname(filePath))}-eval.json`;
    await bucket.file(evalFileName).save(JSON.stringify({
      originalFile: filePath,
      extractedText,
      evaluation: evalJson,
      timestamp: new Date().toISOString(),
    }, null, 2), { contentType: 'application/json' });

    console.log(`Evaluation saved: gs://${bucketName}/${evalFileName}`);

  } catch (error) {
    console.error('Processing failed:', error.message);
    console.error(error.stack);
    // Optional: save error log to GCS or just console
  }
});

// gcloud functions deploy processOCR --gen2 --runtime nodejs22 --region asia-south1 --source functions/process-ocr --entry-point processOCR --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" --trigger-event-filters="bucket=copy-checking-bucket-asia" --service-account ocr-processor-sa@secure-brook-470609-q7.iam.gserviceaccount.com --set-secrets GEMINI_API_KEY=gemini-api-key:latest --memory=512MB --timeout=120s --no-allow-unauthenticated

