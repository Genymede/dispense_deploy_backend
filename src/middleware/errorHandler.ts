import { Request, Response, NextFunction } from 'express';

export class AppError extends Error {
  constructor(public message: string, public statusCode: number = 500) {
    super(message);
    this.name = 'AppError';
  }
}

export function errorHandler(
  err: Error,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  if (err instanceof AppError) {
    res.status(err.statusCode).json({ error: err.message });
    return;
  }

  // PostgreSQL errors
  const pgErr = err as any;
  if (pgErr.code) {
    switch (pgErr.code) {
      case '23503':
        res.status(400).json({ error: 'ข้อมูลอ้างอิงไม่ถูกต้อง (Foreign Key)', detail: pgErr.detail });
        return;
      case '23505':
        res.status(409).json({ error: 'ข้อมูลซ้ำ', detail: pgErr.detail });
        return;
      case '23514':
        res.status(400).json({ error: 'ข้อมูลไม่ถูกต้องตามเงื่อนไข', detail: pgErr.detail });
        return;
    }
  }

  console.error('[Unhandled Error]', err);
  res.status(500).json({ error: 'เกิดข้อผิดพลาดภายในระบบ' });
}

export function notFound(req: Request, res: Response): void {
  res.status(404).json({ error: `ไม่พบ endpoint: ${req.method} ${req.path}` });
}
