import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import dataRoutes from './routes/data.routes.js';
import qaRoutes from './routes/qa.routes.js';
import rateLimit from 'express-rate-limit';
const app = express();

const uploadLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,  // 15 min
  max:      20,               // 20 uploads per 15 min per IP
  message:  { success: false, message: 'Too many requests' },
});

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max:      100,
});

app.use(morgan('dev'));
app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({
    success: true,
    message: "API is running 🚀",
  });
});

app.use('/api/v1',uploadLimiter , dataRoutes );   // recive pdf and jpeg file from frontend and send it to controller for processing
app.use('/api/v1/qa', apiLimiter,qaRoutes );

app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: "Route not found",
  });
});

export default app;