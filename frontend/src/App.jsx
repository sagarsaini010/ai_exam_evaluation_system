import { useMemo, useState } from 'react';
import UploadFlow from './components/UploadFlow';
import QAIngestFlow from './components/QAIngestFlow';

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

export default function App() {
  const [tab, setTab] = useState('upload');

  const title = useMemo(
    () => (tab === 'upload' ? 'Student Copy Upload & Tracking' : 'Teacher Q&A Ingestion'),
    [tab]
  );

  return (
    <main className="container">
      <header className="header">
        <h1>AI Exam Evaluation System</h1>
        <p className="muted">Backend: {API_BASE}</p>
      </header>

      <section className="tabs">
        <button
          className={tab === 'upload' ? 'tab active' : 'tab'}
          onClick={() => setTab('upload')}
        >
          Upload Flow
        </button>
        <button
          className={tab === 'qa' ? 'tab active' : 'tab'}
          onClick={() => setTab('qa')}
        >
          QA Flow
        </button>
      </section>

      <h2>{title}</h2>
      {tab === 'upload' ? <UploadFlow apiBase={API_BASE} /> : <QAIngestFlow apiBase={API_BASE} />}
    </main>
  );
}
