import { Request, Response, NextFunction } from 'express';
import { query, pool, SCHEMA } from '../db/pool';

// ── Helper: atomic queue number increment ─────────────────────────────────────
// Returns formatted queue number e.g. "A001". Must be called inside a transaction.
export async function nextQueueNumber(client: any): Promise<string> {
  const { rows } = await client.query(
    `UPDATE ${SCHEMA}.system_settings
     SET value = (value::int + 1)::text, updated_at = NOW()
     WHERE key = 'queue_current_number'
     RETURNING value::int AS num`
  );
  const num = rows[0]?.num ?? 1;
  return `A${String(num).padStart(3, '0')}`;
}

// ── GET /queue ────────────────────────────────────────────────────────────────
export async function getQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { status } = req.query;
    let where = `DATE(qe.created_at) = CURRENT_DATE`;
    const params: any[] = [];
    if (status) {
      params.push(status);
      where += ` AND qe.status = $${params.length}`;
    }

    const { rows } = await query(
      `SELECT
         qe.queue_id, qe.queue_number, qe.status, qe.note,
         qe.called_at, qe.completed_at, qe.created_at,
         qe.patient_id,
         CASE WHEN pa.patient_id IS NOT NULL THEN CONCAT(pa.first_name,' ',pa.last_name) END AS patient_name,
         pa.hn_number,
         CASE WHEN pu.id IS NOT NULL THEN COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') END AS called_by_name,
         CASE
           WHEN qe.patient_id IS NULL THEN true
           WHEN EXISTS (
             SELECT 1 FROM ${SCHEMA}.prescriptions pr
             WHERE pr.patient_id = qe.patient_id
               AND pr.status = 'pending'
           ) THEN false
           ELSE true
         END AS can_call,
         COUNT(*) OVER() AS total_count
       FROM ${SCHEMA}.queue_entries qe
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = qe.patient_id
       LEFT JOIN public.profiles pu ON pu.id = qe.called_by
       LEFT JOIN auth.users     au ON au.id = qe.called_by
       WHERE ${where}
       ORDER BY qe.queue_id ASC`,
      params
    );
    const total = parseInt(rows[0]?.total_count ?? '0');
    res.json({ data: rows, total });
  } catch (err) { next(err); }
}

// ── GET /queue/display ────────────────────────────────────────────────────────
export async function getQueueDisplay(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows: calledRows } = await query(
      `SELECT
         qe.queue_id, qe.queue_number, qe.status, qe.called_at,
         CASE WHEN pa.patient_id IS NOT NULL THEN CONCAT(pa.first_name,' ',pa.last_name) END AS patient_name,
         pa.hn_number
       FROM ${SCHEMA}.queue_entries qe
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = qe.patient_id
       WHERE DATE(qe.created_at) = CURRENT_DATE AND qe.status = 'called'
       ORDER BY qe.called_at DESC
       LIMIT 1`
    );

    const { rows: waitingRows } = await query(
      `SELECT
         qe.queue_id, qe.queue_number,
         CASE WHEN pa.patient_id IS NOT NULL THEN CONCAT(pa.first_name,' ',pa.last_name) END AS patient_name,
         pa.hn_number
       FROM ${SCHEMA}.queue_entries qe
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = qe.patient_id
       WHERE DATE(qe.created_at) = CURRENT_DATE AND qe.status = 'waiting'
       ORDER BY qe.queue_id ASC
       LIMIT 5`
    );

    res.json({
      current: calledRows[0] ?? null,
      waiting: waitingRows,
    });
  } catch (err) { next(err); }
}

// ── GET /queue/stats ──────────────────────────────────────────────────────────
export async function getQueueStats(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(
      `SELECT
         COUNT(*) FILTER (WHERE status = 'waiting')   AS waiting,
         COUNT(*) FILTER (WHERE status = 'called')    AS called,
         COUNT(*) FILTER (WHERE status = 'completed') AS completed,
         COUNT(*) FILTER (WHERE status = 'skipped')   AS skipped,
         COUNT(*) AS total
       FROM ${SCHEMA}.queue_entries
       WHERE DATE(created_at) = CURRENT_DATE`
    );
    const r = rows[0];
    // also return current counter value
    const { rows: settingRows } = await query(
      `SELECT value::int AS current_number FROM ${SCHEMA}.system_settings WHERE key = 'queue_current_number'`
    );
    res.json({
      waiting:        parseInt(r.waiting ?? '0'),
      called:         parseInt(r.called ?? '0'),
      completed:      parseInt(r.completed ?? '0'),
      skipped:        parseInt(r.skipped ?? '0'),
      total:          parseInt(r.total ?? '0'),
      current_number: settingRows[0]?.current_number ?? 0,
    });
  } catch (err) { next(err); }
}

// ── POST /queue ───────────────────────────────────────────────────────────────
export async function createQueue(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { patient_id, note } = req.body;

    // Auto-reset counter when no queues exist for today yet (new Bangkok day)
    // Uses pure SQL to avoid JS/Node timezone issues
    await client.query(
      `UPDATE ${SCHEMA}.system_settings
       SET value = '0', updated_at = NOW()
       WHERE key = 'queue_current_number'
         AND NOT EXISTS (
           SELECT 1 FROM ${SCHEMA}.queue_entries
           WHERE DATE(created_at AT TIME ZONE 'Asia/Bangkok')
               = (NOW() AT TIME ZONE 'Asia/Bangkok')::date
         )`
    );

    const queue_number = await nextQueueNumber(client);

    const { rows } = await client.query(
      `INSERT INTO ${SCHEMA}.queue_entries (queue_number, patient_id, note)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [queue_number, patient_id || null, note || null]
    );

    // Fetch patient name if patient_id provided
    let patient_name: string | null = null;
    let hn_number: string | null = null;
    if (patient_id) {
      const { rows: pa } = await client.query(
        `SELECT CONCAT(first_name,' ',last_name) AS patient_name, hn_number FROM ${SCHEMA}.patient WHERE patient_id = $1`,
        [patient_id]
      );
      patient_name = pa[0]?.patient_name ?? null;
      hn_number    = pa[0]?.hn_number ?? null;
    }

    await client.query('COMMIT');
    res.status(201).json({ ...rows[0], patient_name, hn_number });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── POST /queue/reset ─────────────────────────────────────────────────────────
export async function resetQueue(req: Request, res: Response, next: NextFunction) {
  try {
    // Reset counter
    await query(
      `UPDATE ${SCHEMA}.system_settings SET value = '0', updated_at = NOW() WHERE key = 'queue_current_number'`
    );
    // Delete today's non-completed entries (waiting + called)
    const { rows } = await query(
      `DELETE FROM ${SCHEMA}.queue_entries
       WHERE DATE(created_at) = CURRENT_DATE AND status IN ('waiting','called')
       RETURNING queue_id`
    );
    res.json({ success: true, deleted: rows.length, message: `รีเซ็ตคิวแล้ว (ลบ ${rows.length} รายการ)` });
  } catch (err) { next(err); }
}

// ── PUT /queue/:id/call ───────────────────────────────────────────────────────
export async function callQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { called_by } = req.body;

    const { rows } = await query(
      `UPDATE ${SCHEMA}.queue_entries
       SET status = 'called', called_at = NOW(), called_by = $2
       WHERE queue_id = $1 AND status IN ('waiting','called')
       RETURNING *`,
      [id, called_by || null]
    );
    if (!rows.length) {
      return res.status(400).json({ error: 'ไม่พบคิว หรือคิวนี้ไม่อยู่ในสถานะที่เรียกได้' });
    }
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── PUT /queue/:id/complete ───────────────────────────────────────────────────
export async function completeQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.queue_entries
       SET status = 'completed', completed_at = NOW()
       WHERE queue_id = $1 AND status IN ('waiting','called')
       RETURNING *`,
      [id]
    );
    if (!rows.length) {
      return res.status(400).json({ error: 'ไม่พบคิว หรือคิวนี้ถูกปิดแล้ว' });
    }
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── PUT /queue/:id/skip ───────────────────────────────────────────────────────
export async function skipQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.queue_entries
       SET status = 'skipped'
       WHERE queue_id = $1 AND status IN ('waiting','called')
       RETURNING *`,
      [id]
    );
    if (!rows.length) {
      return res.status(400).json({ error: 'ไม่พบคิว หรือคิวนี้ถูกปิดแล้ว' });
    }
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── PUT /queue/:id/receive ────────────────────────────────────────────────────
// ยืนยันว่าผู้ป่วยรับยาแล้วจริง (completed + recorded received_at/received_by)
export async function receiveQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { received_by } = req.body;

    const { rows } = await query(
      `UPDATE ${SCHEMA}.queue_entries
       SET status = 'completed',
           completed_at = NOW(),
           received_at  = NOW(),
           received_by  = $2
       WHERE queue_id = $1 AND status IN ('waiting','called')
       RETURNING *`,
      [id, received_by || null]
    );
    if (!rows.length) {
      return res.status(400).json({ error: 'ไม่พบคิว หรือคิวนี้ถูกปิดแล้ว' });
    }
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── DELETE /queue/:id ─────────────────────────────────────────────────────────
export async function deleteQueue(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { rows } = await query(
      `DELETE FROM ${SCHEMA}.queue_entries
       WHERE queue_id = $1 AND status = 'waiting'
       RETURNING queue_id`,
      [id]
    );
    if (!rows.length) {
      return res.status(400).json({ error: 'ลบได้เฉพาะคิวที่กำลังรอเท่านั้น' });
    }
    res.json({ success: true });
  } catch (err) { next(err); }
}
