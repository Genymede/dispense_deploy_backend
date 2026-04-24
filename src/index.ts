import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import dotenv from 'dotenv';
import { testConnection, query, SCHEMA } from './db/pool';
import router from './routes/index';
import { verifyToken } from './controllers/auth.controller';
import { errorHandler, notFound } from './middleware/errorHandler';
import { startScheduler } from './scheduler';

dotenv.config();

const app = express();
const PORT = parseInt(process.env.PORT || '3001');

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(helmet());
app.use(cors({
  origin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));
app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev'));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ── Safe schema additions (idempotent, run on every start) ───────────────────
async function ensureColumns() {
  const safeAlters = [
    `ALTER TABLE ${SCHEMA}.prescriptions ADD COLUMN IF NOT EXISTS diagnosis TEXT`,
    `ALTER TABLE ${SCHEMA}.prescriptions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`,
  ];
  for (const sql of safeAlters) {
    try { await query(sql); } catch { /* already exists */ }
  }
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.use('/api', verifyToken, router);

// ── 404 + Error ───────────────────────────────────────────────────────────────
app.use(notFound);
app.use(errorHandler);

// ── Start ─────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await testConnection().then(() => ensureColumns());
    app.listen(PORT, () => {
      console.log(`🚀 PharmSub API listening on http://localhost:${PORT}/api`);
      console.log(`   Health: http://localhost:${PORT}/api/health`);
      console.log(`   Env:    ${process.env.NODE_ENV || 'development'}`);
    });
    startScheduler();
  } catch (err) {
    console.error('❌ Failed to start server:', err);
    process.exit(1);
  }
}

start();

export default app;
