import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// ── Primary: DATABASE_URL_PG (Supabase pooler)
// ── Fallback: individual env vars (local dev)
const connectionString = process.env.DATABASE_URL_PG || undefined;

const pool = connectionString
  ? new Pool({
      connectionString,
      ssl: { rejectUnauthorized: false },   // Supabase requires SSL
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    })
  : new Pool({
      host:     process.env.DB_HOST     || 'localhost',
      port:     parseInt(process.env.DB_PORT || '5432'),
      database: process.env.DB_NAME     || 'postgres',
      user:     process.env.DB_USER     || 'postgres',
      password: process.env.DB_PASSWORD || '',
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

export { pool };

// ── Schema name — change here if needed ──────────────────────
export const SCHEMA = process.env.DB_SCHEMA || 'dispense_02';

pool.on('error', (err) => {
  console.error('[DB] Idle client error:', err.message);
});

// ── query wrapper ─────────────────────────────────────────────
export async function query<T = any>(text: string, params?: any[]) {
  const start = Date.now();
  try {
    const res = await pool.query(text, params);
    if (process.env.NODE_ENV === 'development') {
      console.log(`[DB ${Date.now() - start}ms] ${text.slice(0, 120).replace(/\n\s+/g, ' ')}`);
    }
    return { rows: res.rows as T[], rowCount: res.rowCount ?? 0 };
  } catch (err) {
    console.error('[DB Error]', (err as Error).message);
    throw err;
  }
}

// ── test connection ───────────────────────────────────────────
export async function testConnection() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query('SELECT NOW() AS now, current_schema() AS schema');
    const target = connectionString ? 'Supabase (pooler)' : 'local';
    console.log(`✅ DB connected [${target}] | schema=${SCHEMA} | server time=${rows[0].now}`);
  } finally {
    client.release();
  }
}

// ── resolver helpers — email/uuid → auth.users.id (UUID) ─────
// ใช้ใน controllers เพื่อแปลง email → UUID หรือ pass-through UUID ที่ได้จาก JWT

export async function resolveUserId(uuidOrEmail: string | number | undefined): Promise<string | null> {
  if (!uuidOrEmail) return null;
  const s = String(uuidOrEmail);
  // UUID format → ใช้ตรงๆ
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) return s;
  // email → lookup ใน auth.users
  const { rows } = await query<{ id: string }>(
    `SELECT id FROM auth.users WHERE email = $1 LIMIT 1`,
    [s]
  );
  return rows[0]?.id ?? null;
}

export async function resolvePatientId(hnOrId: string | number | undefined): Promise<number | null> {
  if (!hnOrId) return null;
  if (typeof hnOrId === 'number') return hnOrId;
  if (/^\d+$/.test(String(hnOrId)) && String(hnOrId).length < 8) return parseInt(String(hnOrId));
  const { rows } = await query<{ patient_id: number }>(
    `SELECT patient_id FROM ${SCHEMA}.patient WHERE hn_number = $1 OR national_id = $1 LIMIT 1`,
    [String(hnOrId)]
  );
  return rows[0]?.patient_id ?? null;
}

export async function resolveMedId(nameOrId: string | number | undefined): Promise<number | null> {
  if (!nameOrId) return null;
  if (typeof nameOrId === 'number') return nameOrId;
  if (/^\d+$/.test(String(nameOrId))) return parseInt(String(nameOrId));
  const { rows } = await query<{ med_id: number }>(
    `SELECT med_id FROM ${SCHEMA}.med_table
     WHERE med_name ILIKE $1 OR med_generic_name ILIKE $1 OR med_marketing_name ILIKE $1
     ORDER BY med_name LIMIT 1`,
    [String(nameOrId)]
  );
  return rows[0]?.med_id ?? null;
}

// ── paginate — shared helper สำหรับทุก controller ────────────────────────────
export function paginate(page: unknown, limit: unknown) {
  const p = Math.max(1, parseInt(String(page || 1)));
  const l = Math.min(200, Math.max(1, parseInt(String(limit || 50))));
  return { limit: l, offset: (p - 1) * l, page: p };
}
