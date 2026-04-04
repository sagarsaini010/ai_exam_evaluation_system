import { useState } from 'react';

export default function QAIngestFlow({ apiBase }) {
  const [examId, setExamId] = useState('');
  const [subjectId, setSubjectId] = useState('');
  const [pdf, setPdf] = useState(null);
  const [questions, setQuestions] = useState([]);
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const ingestPdf = async () => {
    if (!pdf) return setError('Please select a PDF first.');
    setError('');
    setLoading(true);
    setResult(null);

    try {
      const formData = new FormData();
      formData.append('examId', examId);
      formData.append('subjectId', subjectId);
      formData.append('pdf', pdf);

      const res = await fetch(`${apiBase}/api/v1/qa/ingest-pdf`, {
        method: 'POST',
        body: formData,
      });
      const data = await res.json();

      if (!res.ok) throw new Error(data.message || 'Ingest failed.');
      setResult(data);
      setQuestions(data.questions || []);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const updateQuestion = (index, key, value) => {
    setQuestions((prev) =>
      prev.map((q, i) => (i === index ? { ...q, [key]: key === 'maxMarks' ? Number(value) : value } : q))
    );
  };

  const confirmQuestions = async () => {
    setLoading(true);
    setError('');

    try {
      const res = await fetch(`${apiBase}/api/v1/qa/confirm`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ examId, subjectId, questions }),
      });
      const data = await res.json();
      if (!res.ok && res.status !== 207) throw new Error(data.message || 'Confirm failed');
      setResult(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="card">
      <label>
        examId
        <input value={examId} onChange={(e) => setExamId(e.target.value)} />
      </label>
      <label>
        subjectId
        <input value={subjectId} onChange={(e) => setSubjectId(e.target.value)} />
      </label>
      <label>
        Question paper PDF
        <input type="file" accept="application/pdf" onChange={(e) => setPdf(e.target.files?.[0] || null)} />
      </label>
      <button onClick={ingestPdf} disabled={loading}>{loading ? 'Processing...' : 'Ingest PDF'}</button>

      {questions.length > 0 && (
        <>
          <h3>Edit questions before confirm</h3>
          {questions.map((q, i) => (
            <div className="question" key={q.questionNo || i}>
              <strong>Q{q.questionNo}</strong>
              <textarea value={q.question || ''} onChange={(e) => updateQuestion(i, 'question', e.target.value)} />
              <textarea value={q.modelAnswer || ''} onChange={(e) => updateQuestion(i, 'modelAnswer', e.target.value)} />
              <input
                type="number"
                min="1"
                value={q.maxMarks ?? ''}
                onChange={(e) => updateQuestion(i, 'maxMarks', e.target.value)}
                placeholder="max marks"
              />
            </div>
          ))}

          <button onClick={confirmQuestions} disabled={loading}>
            {loading ? 'Confirming...' : 'Confirm & Store Q/A'}
          </button>
        </>
      )}

      {error && <p className="error">{error}</p>}
      {result && <pre className="output">{JSON.stringify(result, null, 2)}</pre>}
    </section>
  );
}
