import { PubSub }     from '@google-cloud/pubsub';
import { Storage }   from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import dotenv        from 'dotenv';
import { correctOCRText }        from "./ocrCorrector.js";
import { segmentAnswersWithLLM } from "./answerSegmenter.js";
import { gradeAllAnswers }       from '../services/grading.service.js';
import { checkBatchOperation, parseBatchOutput } from '../../functions/process-ocr/batchOcr.js';

dotenv.config();

const PROJECT_ID        = process.env.GCP_PROJECT_ID      || 'secure-brook-470609-q7';
const SUBSCRIPTION      = process.env.OCR_SUBSCRIPTION    || 'exam-ocr-subscription';
const MAX_MESSAGES      = parseInt(process.env.WORKER_MAX_MESSAGES    || '5',     10);
const DOWNLOAD_TIMEOUT  = parseInt(process.env.DOWNLOAD_TIMEOUT_MS    || '20000', 10);
const OCR_LLM_TIMEOUT   = parseInt(process.env.OCR_LLM_TIMEOUT_MS     || '60000', 10);
const SEG_LLM_TIMEOUT   = parseInt(process.env.SEG_LLM_TIMEOUT_MS     || '60000', 10);
const BATCH_POLL_INTERVAL_MS  = parseInt(process.env.BATCH_POLL_INTERVAL_MS  || '60000', 10);
const BATCH_MAX_POLL_ATTEMPTS = parseInt(process.env.BATCH_MAX_POLL_ATTEMPTS || '20',    10);

const pubsub    = new PubSub({ projectId: PROJECT_ID });
const storage   = new Storage({ projectId: PROJECT_ID });
const firestore = new Firestore({ projectId: PROJECT_ID });

const log = {
  info:  (event, fields = {}) => console.log  (JSON.stringify({ severity: 'INFO',    event, ts: new Date().toISOString(), ...fields })),
  warn:  (event, fields = {}) => console.warn (JSON.stringify({ severity: 'WARNING', event, ts: new Date().toISOString(), ...fields })),
  error: (event, fields = {}) => console.error(JSON.stringify({ severity: 'ERROR',   event, ts: new Date().toISOString(), ...fields })),
  debug: (event, fields = {}) => console.log  (JSON.stringify({ severity: 'DEBUG',   event, ts: new Date().toISOString(), ...fields })),
};

function safeJSONParse(str) {
  try   { return JSON.parse(str); }
  catch { return null; }
}

function withTimeout(promise, ms = 30_000, label = 'operation') {
  const timeout = new Promise((_, reject) =>
    setTimeout(
      () => reject(Object.assign(new Error(`Timeout: ${label} exceeded ${ms}ms`), { code: 'ETIMEDOUT' })),
      ms
    )
  );
  return Promise.race([promise, timeout]);
}

function isRetryableError(err) {
  if (!err) return false;
  if (['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'].includes(err.code)) return true;
  if (err.message?.startsWith('Timeout:'))  return true;
  const status = err.status ?? err.response?.status;
  if (status >= 500)   return true;
  if (status === 429)  return true;
  return false;
}

function docId(ocrPath) {
  return Buffer.from(ocrPath).toString('base64url');
}

async function markProcessedAndAck(ocrPath, message, extraFields = {}) {
  await firestore
    .collection('ocr_processed')
    .doc(docId(ocrPath))
    .set(
      { ocrPath, processedAt: new Date().toISOString(), ...extraFields },
      { merge: true }
    );
  message.ack();
}

/* ─── Layout → plain text converter ──────────────────────────────────────────
   LLM ko layout-aware text deta hai:
   "Page 1 (confidence: 0.81):\nline1\nline2\n\nPage 2..."
   Isse LLM ko page structure pata rehta hai, plain blob nahi milta.
─────────────────────────────────────────────────────────────────────────── */
function layoutToLLMText(pages) {
  if (!pages || pages.length === 0) return null;

  return pages
    .map((p) => {
      const header = `--- Page ${p.page} (confidence: ${p.confidence ?? 'N/A'}) ---`;
      const body   = (p.lines || []).join('\n');
      return `${header}\n${body}`;
    })
    .join('\n\n');
}

let inFlight     = 0;
let shuttingDown = false;

function beginWork() {
  if (shuttingDown) return false;
  inFlight++;
  return true;
}

function endWork() {
  inFlight--;
}

function waitForInFlight(pollMs = 100) {
  return new Promise((resolve) => {
    const check = () => (inFlight === 0 ? resolve() : setTimeout(check, pollMs));
    check();
  });
}

// ─── Main message handler ─────────────────────────────────────────────────────
async function handleMessage(message) {
  const messageId  = message.id;
  const rawPayload = message.data.toString();
  const startedAt  = Date.now();

  log.info('WORKER_MESSAGE_RECEIVED', { messageId });

  const data = safeJSONParse(rawPayload);

  if (!data?.bucket || !data?.ocrPath) {
    log.error('WORKER_INVALID_PAYLOAD', { messageId, rawPayload: rawPayload.slice(0, 500) });
    message.ack();
    return;
  }

  const { bucket, ocrPath } = data;

  log.info('WORKER_PAYLOAD_PARSED', { messageId, bucket, ocrPath });

  const processedDocRef = firestore.collection('ocr_processed').doc(docId(ocrPath));

  log.info('CHECKING_IDEMPOTENCY', {
    messageId,
    ocrPath,
    docId:    docId(ocrPath),
    fullPath: `ocr_processed/${docId(ocrPath)}`,
  });

  let snap;
  try {
    snap = await withTimeout(
      processedDocRef.get(),
      10_000,
      'Firestore idempotency check'
    );
  } catch (err) {
    log.error('FIRESTORE_IDEMPOTENCY_CHECK_FAILED', {
      messageId,
      ocrPath,
      error:   err.message,
      code:    err.code,
      details: err.details,
    });
    throw err;
  }

  if (snap.exists) {
    log.warn('WORKER_DUPLICATE_MESSAGE', { messageId, ocrPath });
    message.ack();
    return;
  }

  log.info('GCS_DOWNLOAD_START', { messageId, bucket, ocrPath });

  let fileBuffer;
  try {
    [fileBuffer] = await withTimeout(
      storage.bucket(bucket).file(ocrPath).download(),
      DOWNLOAD_TIMEOUT,
      `GCS download ${ocrPath}`
    );
  } catch (err) {
    log.error('GCS_DOWNLOAD_FAILED', {
      messageId,
      bucket,
      ocrPath,
      error: err.message,
      code:  err.code,
    });
    throw err;
  }

  log.info('GCS_DOWNLOAD_COMPLETE', { messageId, ocrPath, bytes: fileBuffer.length });

  const ocrJson = safeJSONParse(fileBuffer.toString());

  const student = data.student ?? ocrJson?.student ?? null;

  // ── Invalid OCR JSON ──────────────────────────────────────────────────────
  if (!ocrJson?.text) {
    log.error('WORKER_INVALID_OCR_JSON', { messageId, ocrPath });
    if (data.jobId) {
      await firestore.collection('exam_jobs').doc(data.jobId).update({
        status: 'failed',
        error:  'Invalid OCR JSON',
      }).catch(() => {});
    }
    await markProcessedAndAck(ocrPath, message, { skippedReason: 'invalid_ocr_json' });
    return;
  }

  const rawText = ocrJson.text;

  if (!rawText?.trim()) {
    log.error('WORKER_EMPTY_OCR_TEXT', { messageId, ocrPath });
    await markProcessedAndAck(ocrPath, message, { skippedReason: 'empty_text' });
    return;
  }

  const cleanedText = rawText.replace(/\s+/g, ' ').trim();

  // ── Layout pages ──────────────────────────────────────────────────────────
  const layoutPages = ocrJson.pages || [];

  log.info('WORKER_LAYOUT_PAGES', {
    messageId,
    ocrPath,
    totalLayoutPages: layoutPages.length,
    pages: layoutPages.map(p => ({
      page:       p.page,
      confidence: p.confidence,
      lineCount:  p.lines?.length ?? 0,
      firstLine:  p.lines?.[0]                  ?? null,
      lastLine:   p.lines?.[p.lines.length - 1] ?? null,
    })),
  });

  log.info('WORKER_TEXT_READY', {
    messageId,
    ocrPath,
    textLength:     cleanedText.length,
    totalPages:     ocrJson.totalPages,
    avgConfidence:  ocrJson.avgConfidence  ?? null,
    pageConfidence: ocrJson.pageConfidence ?? null,
    student,
  });

  // ── Layout text banao LLM ke liye ─────────────────────────────────────────
  const llmInputText = layoutPages.length > 0
    ? layoutToLLMText(layoutPages)
    : cleanedText;

  log.info('WORKER_LLM_INPUT_READY', {
    messageId,
    ocrPath,
    source:      layoutPages.length > 0 ? 'layout_pages' : 'cleaned_text',
    inputLength: llmInputText.length,
  });

  // ── LLM OCR correction ────────────────────────────────────────────────────
  let correctedText = llmInputText;

  try {
    log.info('OCR_CORRECTION_START', { messageId, ocrPath, textLength: llmInputText.length });

    correctedText = await withTimeout(
      correctOCRText(llmInputText),
      OCR_LLM_TIMEOUT,
      'OCR correction'
    );

    log.info('OCR_CORRECTION_COMPLETE', {
      messageId,
      ocrPath,
      inputLength:  llmInputText.length,
      outputLength: correctedText.length,
    });

  } catch (err) {
    log.error('OCR_CORRECTION_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      code:  err.code,
      note:  'Falling back to llmInputText',
    });
  }

  // ── LLM answer segmentation ───────────────────────────────────────────────
  let segmentedAnswers = { questions: [] };

  try {
    log.info('ANSWER_SEGMENTATION_START', { messageId, ocrPath, textLength: correctedText.length });

    segmentedAnswers = await withTimeout(
      segmentAnswersWithLLM(correctedText),
      SEG_LLM_TIMEOUT,
      'answer segmentation'
    );

    log.info('ANSWER_SEGMENTATION_COMPLETE', {
      messageId,
      ocrPath,
      questionsFound: segmentedAnswers?.questions?.length ?? 0,
    });

    log.debug('SEGMENTED_ANSWERS_DETAIL', {
      messageId,
      ocrPath,
      segmentedAnswers,
    });

  } catch (err) {
    log.error('ANSWER_SEGMENTATION_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      code:  err.code,
      note:  'Falling back to empty questions array',
    });
  }

  // ── Persist results to Firestore ──────────────────────────────────────────
  log.info('FIRESTORE_WRITE_START', { messageId, ocrPath });

  try {
    await withTimeout(
      firestore.collection('exam_answers').doc(docId(ocrPath)).set({
        ocrPath,
        bucket,
        student:          student          ?? null,
        rawText,
        cleanedText,
        llmInputText,
        correctedText,
        segmentedAnswers,
        totalPages:       ocrJson.totalPages    ?? null,
        avgConfidence:    ocrJson.avgConfidence  ?? null,
        pageConfidence:   ocrJson.pageConfidence ?? null,
        pages:            layoutPages,
        processedAt:      new Date().toISOString(),
        processingTimeMs: Date.now() - startedAt,
      }),
      15_000,
      'Firestore exam_answers write'
    );
  } catch (err) {
    log.error('FIRESTORE_WRITE_FAILED', {
      messageId,
      ocrPath,
      error:   err.message,
      code:    err.code,
      details: err.details,
    });
    throw err;
  }

  log.info('FIRESTORE_WRITE_COMPLETE', { messageId, ocrPath });

  // ── Extract examId + subjectId ────────────────────────────────────────────
  const jobId     = data.jobId     ?? null;
  const examId    = data.examId    ?? ocrJson?.examId    ?? null;
  const subjectId = data.subjectId ?? ocrJson?.subjectId ?? null;

  // ── Grading pipeline ──────────────────────────────────────────────────────
  if (examId && subjectId && segmentedAnswers?.questions?.length > 0) {
    try {
      log.info('GRADING_PIPELINE_START', {
        messageId, jobId, examId, subjectId,
        questionsToGrade: segmentedAnswers.questions.length,
      });

      if (jobId) {
        await firestore.collection('exam_jobs').doc(jobId).update({
          status: 'grading',
        }).catch(err => log.warn('JOB_STATUS_GRADING_UPDATE_FAILED', { jobId, error: err.message }));
      }

      const gradingResult = await gradeAllAnswers(
        segmentedAnswers.questions,
        examId,
        subjectId,
        student,
        jobId
      );

      const resultDocId = `${student?.studentId ?? 'unknown'}_${examId}_${subjectId}`;

      await withTimeout(
        firestore.collection('exam_results').doc(resultDocId).set({
          ...gradingResult,
          ocrPath,
          bucket,
        }),
        15_000,
        'Firestore exam_results write'
      );

      log.info('EXAM_RESULTS_SAVED', { messageId, jobId, resultDocId });

      if (jobId) {
        await withTimeout(
          firestore.collection('exam_jobs').doc(jobId).update({
            status:           'completed',
            questionsFound:   segmentedAnswers?.questions?.length ?? 0,
            avgConfidence:    ocrJson.avgConfidence  ?? null,
            segmentedAnswers: segmentedAnswers,
            gradingStatus:    gradingResult.gradingStatus,
            totalMarks:       gradingResult.totalMarks,
            maxMarks:         gradingResult.maxMarks,
            percentage:       gradingResult.percentage,
            flaggedQuestions: gradingResult.flaggedQuestions,
            gradedAnswers:    gradingResult.gradedAnswers,
            gradedAt:         gradingResult.gradedAt,
            processedAt:      new Date().toISOString(),
            processingTimeMs: Date.now() - startedAt,
          }),
          10_000,
          'Firestore exam_jobs grading update'
        );
        log.info('JOB_STATUS_UPDATED', { messageId, jobId, status: 'completed' });
      }

      log.info('GRADING_PIPELINE_COMPLETE', {
        messageId,
        jobId,
        gradingStatus: gradingResult.gradingStatus,
        totalMarks:    gradingResult.totalMarks,
        maxMarks:      gradingResult.maxMarks,
        percentage:    gradingResult.percentage,
        flaggedCount:  gradingResult.flaggedQuestions?.length ?? 0,
      });

    } catch (err) {
      log.error('GRADING_PIPELINE_FAILED', {
        messageId, jobId, error: err.message, code: err.code,
      });

      if (jobId) {
        await firestore.collection('exam_jobs').doc(jobId).update({
          status:           'completed',
          questionsFound:   segmentedAnswers?.questions?.length ?? 0,
          avgConfidence:    ocrJson.avgConfidence ?? null,
          segmentedAnswers: segmentedAnswers,
          gradingStatus:    'failed',
          gradingError:     err.message,
          processedAt:      new Date().toISOString(),
          processingTimeMs: Date.now() - startedAt,
        }).catch(() => {});
      }
    }

  } else {
    log.warn('GRADING_SKIPPED', {
      messageId,
      jobId,
      reason: !examId
        ? 'no_examId_in_message'
        : !subjectId
        ? 'no_subjectId_in_message'
        : 'no_questions_found',
    });

    if (jobId) {
      await withTimeout(
        firestore.collection('exam_jobs').doc(jobId).update({
          status:           'completed',
          questionsFound:   segmentedAnswers?.questions?.length ?? 0,
          avgConfidence:    ocrJson.avgConfidence  ?? null,
          segmentedAnswers: segmentedAnswers,
          processedAt:      new Date().toISOString(),
          processingTimeMs: Date.now() - startedAt,
        }),
        10_000,
        'Firestore exam_jobs update'
      ).catch(err => log.warn('JOB_STATUS_UPDATE_FAILED', { messageId, jobId, error: err.message }));
    }
  }

  // ── Mark processed + ack ──────────────────────────────────────────────────
  try {
    await markProcessedAndAck(ocrPath, message, {
      student:          student ?? null,
      questionsFound:   segmentedAnswers?.questions?.length ?? 0,
      processingTimeMs: Date.now() - startedAt,
    });
  } catch (err) {
    log.error('MARK_PROCESSED_FAILED', {
      messageId,
      ocrPath,
      error: err.message,
      note:  'Results saved but idempotency record may be missing',
    });
    throw err;
  }

  log.info('WORKER_MESSAGE_PROCESSED', {
    messageId,
    ocrPath,
    student,
    questionsFound:   segmentedAnswers?.questions?.length ?? 0,
    processingTimeMs: Date.now() - startedAt,
  });
}

// ─── Pub/Sub subscription ─────────────────────────────────────────────────────
const subscription = pubsub.subscription(SUBSCRIPTION, {
  flowControl: { maxMessages: MAX_MESSAGES },
});

log.info('WORKER_STARTED', {
  subscription:    SUBSCRIPTION,
  maxMessages:     MAX_MESSAGES,
  projectId:       PROJECT_ID,
  downloadTimeout: DOWNLOAD_TIMEOUT,
  ocrLlmTimeout:   OCR_LLM_TIMEOUT,
  segLlmTimeout:   SEG_LLM_TIMEOUT,
  batchPollIntervalMs:  BATCH_POLL_INTERVAL_MS,
  batchMaxPollAttempts: BATCH_MAX_POLL_ATTEMPTS,
});

subscription.on('message', async (message) => {
  if (!beginWork()) {
    log.warn('WORKER_SHUTTING_DOWN_NACK', { messageId: message.id });
    message.nack();
    return;
  }

  try {
    await handleMessage(message);
  } catch (err) {
    const messageId = message.id;

    if (isRetryableError(err)) {
      log.warn('WORKER_RETRYABLE_ERROR', {
        messageId,
        error: err.message,
        code:  err.code,
        note:  'NACKing — Pub/Sub will redeliver',
      });
      message.nack();
    } else {
      log.error('WORKER_PERMANENT_ERROR', {
        messageId,
        error: err.message,
        code:  err.code,
        note:  'ACKing — will not redeliver to avoid infinite loop',
      });
      message.ack();
    }
  } finally {
    endWork();
  }
});

subscription.on('error', (err) => {
  log.error('WORKER_SUBSCRIPTION_ERROR', {
    error:   err.message,
    code:    err.code,
    details: err.details,
  });
});

subscription.on('close', () => {
  log.info('WORKER_SUBSCRIPTION_CLOSED', {});
});

// ─── Batch OCR Poller ─────────────────────────────────────────────────────────

async function pollBatchOperations() {
  if (shuttingDown) return;

  let pending = [];
  try {
    const snap = await firestore
      .collection('ocr_batch_operations')
      .where('status', '==', 'pending')
      .limit(10)
      .get();
    pending = snap.docs;
  } catch (err) {
    log.error('BATCH_POLL_FETCH_FAILED', { error: err.message });
    return;
  }

  if (pending.length === 0) return;

  log.info('BATCH_POLL_CHECK', { pendingCount: pending.length });

  for (const docSnap of pending) {
    const data = docSnap.data();
    const {
      operationName,
      jobId,
      outputBucket,
      outputPrefix,
      sourceFile,
      sourceBucket,
      customMetadata,
      pollAttempts,
    } = data;

    // ── Max attempts exceed — timeout fail ───────────────────────────────
    if ((pollAttempts ?? 0) >= BATCH_MAX_POLL_ATTEMPTS) {
      log.error('BATCH_POLL_MAX_ATTEMPTS', { operationName, jobId, pollAttempts });
      await docSnap.ref.update({ status: 'failed', failedAt: new Date().toISOString() });
      if (jobId) {
        await firestore.collection('exam_jobs').doc(jobId).update({
          status:   'failed',
          error:    `Batch OCR timed out after ${BATCH_MAX_POLL_ATTEMPTS} poll attempts (~${BATCH_MAX_POLL_ATTEMPTS} min)`,
          failedAt: new Date().toISOString(),
        }).catch(() => {});
      }
      continue;
    }

    try {
      // ── Operation status check ────────────────────────────────────────
      const { done, error, individualProcessStatuses } =
        await checkBatchOperation(operationName);

      await docSnap.ref.update({ pollAttempts: (pollAttempts ?? 0) + 1 });

      if (!done) {
        log.info('BATCH_POLL_IN_PROGRESS', {
          operationName, jobId,
          pollAttempts: (pollAttempts ?? 0) + 1,
        });
        continue;
      }

      // ── Operation-level error ─────────────────────────────────────────
      if (error) {
        log.error('BATCH_OPERATION_FAILED', { operationName, jobId, error: error.message });
        await docSnap.ref.update({ status: 'failed', failedAt: new Date().toISOString() });
        if (jobId) {
          await firestore.collection('exam_jobs').doc(jobId).update({
            status:   'failed',
            error:    `Batch OCR operation failed: ${error.message}`,
            failedAt: new Date().toISOString(),
          }).catch(() => {});
        }
        continue;
      }

      // ── Individual document error ─────────────────────────────────────
      const docStatus = individualProcessStatuses?.[0];
      if (docStatus?.status?.code && docStatus.status.code !== 0) {
        const reason = docStatus.status.message || 'Unknown document processing error';
        log.error('BATCH_DOCUMENT_FAILED', { operationName, jobId, reason });
        await docSnap.ref.update({ status: 'failed', failedAt: new Date().toISOString() });
        if (jobId) {
          await firestore.collection('exam_jobs').doc(jobId).update({
            status:   'failed',
            error:    `Batch document failed: ${reason}`,
            failedAt: new Date().toISOString(),
          }).catch(() => {});
        }
        continue;
      }

      log.info('BATCH_OPERATION_COMPLETE', { operationName, jobId });

      // ── Output parse karo ─────────────────────────────────────────────
      let ocrPayload;
      try {
        ocrPayload = await parseBatchOutput({
          outputBucket,
          outputPrefix,
          filePath:       sourceFile,
          jobId,
          customMetadata: customMetadata ?? {},
        });
      } catch (parseErr) {
        log.error('BATCH_OUTPUT_PARSE_FAILED', { operationName, jobId, error: parseErr.message });
        await docSnap.ref.update({ status: 'failed', failedAt: new Date().toISOString() });
        if (jobId) {
          await firestore.collection('exam_jobs').doc(jobId).update({
            status:   'failed',
            error:    `Batch output parse failed: ${parseErr.message}`,
            failedAt: new Date().toISOString(),
          }).catch(() => {});
        }
        continue;
      }

      // ── OCR JSON GCS mein save karo ───────────────────────────────────
      const extIdx     = sourceFile.lastIndexOf('.');
      const outputPath = (extIdx !== -1 ? sourceFile.slice(0, extIdx) : sourceFile) + '-ocr.json';

      try {
        await storage.bucket(sourceBucket).file(outputPath).save(
          JSON.stringify(ocrPayload, null, 2),
          {
            contentType: 'application/json',
            metadata: {
              metadata: {
                ocrGenerated:   'true',
                jobId:          jobId ?? '',
                sourceFile,
                processingMode: 'batch',
              },
            },
          }
        );
        log.info('BATCH_OCR_JSON_SAVED', { operationName, jobId, outputPath });
      } catch (saveErr) {
        log.error('BATCH_OCR_JSON_SAVE_FAILED', { operationName, jobId, error: saveErr.message });
        await docSnap.ref.update({ status: 'failed', failedAt: new Date().toISOString() });
        if (jobId) {
          await firestore.collection('exam_jobs').doc(jobId).update({
            status: 'failed',
            error:  `OCR JSON save to GCS failed: ${saveErr.message}`,
          }).catch(() => {});
        }
        continue;
      }

      // ── Pub/Sub publish — existing worker pipeline trigger hoga ───────
      const pubsubMessage = {
        bucket:      sourceBucket,
        ocrPath:     outputPath,
        sourceFile,
        jobId,
        examId:      customMetadata?.examid    || null,
        subjectId:   customMetadata?.subjectid || null,
        generatedAt: ocrPayload.generatedAt,
        student:     ocrPayload.student,
      };

      let published = false;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await pubsub
            .topic(process.env.OCR_TOPIC || 'exam-ocr-completed')
            .publishMessage({ data: Buffer.from(JSON.stringify(pubsubMessage)) });
          published = true;
          break;
        } catch (pubErr) {
          log.warn('BATCH_PUBSUB_ATTEMPT_FAILED', {
            operationName, jobId, attempt, error: pubErr.message,
          });
          if (attempt < 3) await new Promise(r => setTimeout(r, 500 * attempt));
        }
      }

      if (published) {
        log.info('BATCH_PUBSUB_PUBLISHED', { operationName, jobId, outputPath });
      } else {
        log.error('BATCH_PUBSUB_EXHAUSTED', {
          operationName, jobId, outputPath,
          hint: 'OCR JSON saved in GCS — trigger pipeline manually if needed',
        });
        if (jobId) {
          await firestore.collection('exam_jobs').doc(jobId).update({
            status: 'failed',
            error:  `Pub/Sub publish failed after 3 attempts — OCR JSON saved at gs://${sourceBucket}/${outputPath}`,
          }).catch(() => {});
        }
      }

      // ── Operation record complete mark karo ───────────────────────────
      await docSnap.ref.update({
        status:      'completed',
        completedAt: new Date().toISOString(),
        outputPath,
      });

    } catch (err) {
      // Is iteration ka error — agli operation pe jao, poora poller mat rokna
      log.error('BATCH_POLL_ITERATION_ERROR', {
    operationName, jobId, error: err.message,
  });

  await docSnap.ref.update({
    status: 'failed',
    error: err.message,
    failedAt: new Date().toISOString(),
  });

  if (jobId) {
    await firestore.collection('exam_jobs').doc(jobId).update({
      status: 'failed',
      error: err.message,
      failedAt: new Date().toISOString(),
    }).catch(() => {});
  }
    }
  }
}

// Poller start karo
const batchPoller = setInterval(pollBatchOperations, BATCH_POLL_INTERVAL_MS);

// Startup pe bhi ek baar chalao — restart pe pending operations miss na hon
pollBatchOperations().catch(err =>
  log.error('BATCH_POLL_STARTUP_ERROR', { error: err.message })
);

// ─── Graceful shutdown ────────────────────────────────────────────────────────
async function shutdown(signal) {
  log.info('WORKER_SHUTDOWN_START', { signal, inFlight });
  shuttingDown = true;

  // Poller band karo pehle — naya poll cycle shuru na ho
  clearInterval(batchPoller);
  log.info('WORKER_BATCH_POLLER_STOPPED', { signal });

  try {
    await subscription.close();
    log.info('WORKER_SUBSCRIPTION_STOPPED', { signal });
  } catch (err) {
    log.error('WORKER_SUBSCRIPTION_CLOSE_FAILED', { signal, error: err.message });
  }

  log.info('WORKER_WAITING_FOR_IN_FLIGHT', { signal, inFlight });
  await waitForInFlight();

  log.info('WORKER_SHUTDOWN_COMPLETE', { signal });
  process.exit(0);
}

process.on('unhandledRejection', (reason) => {
  log.error('UNHANDLED_REJECTION', {
    reason: reason?.message ?? String(reason),
    stack:  reason?.stack,
  });
});

process.on('uncaughtException', (err) => {
  log.error('UNCAUGHT_EXCEPTION', {
    error: err.message,
    stack: err.stack,
  });
  process.exit(1);
});

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));