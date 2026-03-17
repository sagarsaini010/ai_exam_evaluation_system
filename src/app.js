import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import dataRoutes from './routes/data.routes.js';
import qaRoutes from './routes/qa.routes.js';

const app = express();

app.use(morgan('dev'));
app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({
    success: true,
    message: "API is running 🚀",
  });
});

app.use('/api/v1', dataRoutes);   // recive pdf and jpeg file from frontend and send it to controller for processing
app.use('/api/v1/qa', qaRoutes);

app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: "Route not found",
  });
});

export default app;