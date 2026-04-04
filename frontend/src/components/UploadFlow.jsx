import { useState } from 'react';

const initialMeta = {
  schoolName: '',
  branchId: '',
  classId: '',
  sectionId: '',
  studentId: '',
  examId: '',
  subjectId: '',
};

export default function UploadFlow({ apiBase }) {
  const [file, setFile] = useState(null);
  const [meta, setMeta] = useState(initialMeta);
  const [jobId, setJobId] = useState('');
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);
  const [polling, setPolling] = useState(false);
  const [error, setError] = useState('');

  const onInput = (key, value) => setMeta((prev) => ({ ...prev, [key]: value }));

  const requestSignedUrl = async () => {
    if (!file) return setError('Please select a file first.');
    setError('');
    setLoading(true);
    setResponse(null);

    try {
      const payload = {
        ...meta,
        fileName: file.name,
        contentType: file.type || 'application/pdf',
      };

      const res = await fetch(`${apiBase}/api/v1/generate-upload-url`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || !data.success) throw new Error(data.message || 'Failed to generate upload URL.');

      const headers = data.requiredHeaders || {};
      const putRes = await fetch(data.uploadUrl, {
        method: 'PUT',
        headers,
        body: file,
      });

      if (!putRes.ok) throw new Error(`Upload failed with status ${putRes.status}`);

      setResponse({ message: 'Upload complete. OCR pipeline triggered.', ...data });
      setJobId(data.jobId || '');
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const checkStatus = async () => {
    if (!jobId) return setError('Enter a jobId first.');
    setPolling(true);
    setError('');

    try {
      const res = await fetch(`${apiBase}/api/v1/status/${jobId}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.message || data.error || 'Failed to fetch status.');
      setResponse(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setPolling(false);
    }
  };

  return (
    <section className="card">
      <h3>1) Generate signed URL and upload file</h3>
      <div className="grid">
        {Object.keys(initialMeta).map((key) => (
          <label key={key}>
            {key}
            <input value={meta[key]} onChange={(e) => onInput(key, e.target.value)} />
          </label>
        ))}
      </div>

      <label>
        exam copy file (pdf/jpg/png)
        <input type="file" accept=".pdf,.jpg,.jpeg,.png" onChange={(e) => setFile(e.target.files?.[0] || null)} />
      </label>

      <button onClick={requestSignedUrl} disabled={loading}>
        {loading ? 'Uploading...' : 'Upload'}
      </button>

      <h3>2) Track evaluation status</h3>
      <label>
        jobId
        <input value={jobId} onChange={(e) => setJobId(e.target.value)} placeholder="Paste job id" />
      </label>
      <button onClick={checkStatus} disabled={polling}>
        {polling ? 'Checking...' : 'Check Status'}
      </button>

      {error && <p className="error">{error}</p>}
      {response && (
        <pre className="output">{JSON.stringify(response, null, 2)}</pre>
      )}
    </section>
  );
}
