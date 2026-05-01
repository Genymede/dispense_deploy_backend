/**
 * scheduler.ts — ตรวจ cut-off period ทุก 1 นาที แล้วรันอัตโนมัติเมื่อถึงเวลา
 */
import { pool, query, SCHEMA } from './db/pool';

export interface CutOffSummary {
  warehouse_name: string;
  newly_expired_count: number;
  near_expiry_count: number;
  low_stock_count: number;
  snapshot_count: number;
}

// ── เวลาปัจจุบันในโซน UTC+7 (ไทย) ───────────────────────────────────────────
function getThaiTime() {
  const now = new Date();
  const thai = new Date(now.getTime() + 7 * 60 * 60 * 1000);
  return {
    month:  thai.getUTCMonth() + 1,
    day:    thai.getUTCDate(),
    hour:   thai.getUTCHours(),
    minute: thai.getUTCMinutes(),
  };
}

// ── logic จริงของการตัดรอบ — ใช้ทั้งใน scheduler และ manual execute ──────────
export async function runCutOffLogic(warehouseId: number, warehouseName: string): Promise<CutOffSummary> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}`);

    // 1. Mark newly expired items ในคลังนี้
    const { rows: newlyExpired } = await client.query(
      `UPDATE med_subwarehouse
       SET is_expired = TRUE, updated_at = NOW()
       WHERE sub_warehouse_id = $1
         AND exp_date < CURRENT_DATE
         AND is_expired = FALSE
       RETURNING med_sid, med_showname, med_quantity, exp_date`,
      [warehouseId]
    );

    // 2. Snapshot: บันทึก closing-balance ลง stock_transactions (quantity=0 = ไม่เปลี่ยนสต็อก)
    const refNo = `CUTOFF-${warehouseId}-${new Date().toISOString().slice(0, 10)}`;
    const { rowCount: snapshotCount } = await client.query(
      `INSERT INTO stock_transactions
         (med_sid, med_id, tx_type, quantity, balance_before, balance_after, source_type, reference_no, note)
       SELECT
         ms.med_sid, ms.med_id, 'adjust', 0,
         ms.med_quantity, ms.med_quantity,
         'cut_off', $2,
         'ยอดปิดงวด ตัดรอบอัตโนมัติ ' || to_char(NOW() AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY HH24:MI')
       FROM med_subwarehouse ms
       WHERE ms.sub_warehouse_id = $1`,
      [warehouseId, refNo]
    );

    // 3. Alert: expired (มีสต็อกเหลืออยู่)
    await client.query(
      `INSERT INTO alert_log (med_sid, alert_type, severity, is_read)
       SELECT med_sid, 'expired', 'critical', FALSE
       FROM med_subwarehouse
       WHERE sub_warehouse_id = $1
         AND (is_expired = TRUE OR exp_date < CURRENT_DATE)
         AND med_quantity > 0
       ON CONFLICT (med_sid, alert_type) DO NOTHING`,
      [warehouseId]
    );

    // 4. Alert: near_expiry (ภายใน 30 วัน)
    const { rowCount: nearExpiryCount } = await client.query(
      `INSERT INTO alert_log (med_sid, alert_type, severity, is_read)
       SELECT med_sid, 'near_expiry', 'warning', FALSE
       FROM med_subwarehouse
       WHERE sub_warehouse_id = $1
         AND exp_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
         AND is_expired = FALSE
         AND med_quantity > 0
       ON CONFLICT (med_sid, alert_type) DO NOTHING`,
      [warehouseId]
    );

    // 5. Alert: low_stock — upsert severity เพื่ออัปเดตถ้า critical มากขึ้น
    const { rowCount: lowStockCount } = await client.query(
      `INSERT INTO alert_log (med_sid, alert_type, severity, is_read)
       SELECT med_sid,
              'low_stock',
              CASE WHEN med_quantity < min_quantity * 0.5 THEN 'critical' ELSE 'warning' END,
              FALSE
       FROM med_subwarehouse
       WHERE sub_warehouse_id = $1
         AND min_quantity IS NOT NULL
         AND med_quantity < min_quantity
         AND med_quantity >= 0
         AND is_expired = FALSE
       ON CONFLICT (med_sid, alert_type)
         DO UPDATE SET severity = EXCLUDED.severity`,
      [warehouseId]
    );

    await client.query('COMMIT');

    // 6. pg_notify (fire-and-forget นอก transaction)
    const payload = JSON.stringify({
      event:                'cut_off_complete',
      warehouse_id:         warehouseId,
      warehouse_name:       warehouseName,
      newly_expired_count:  newlyExpired.length,
      near_expiry_count:    nearExpiryCount ?? 0,
      low_stock_count:      lowStockCount ?? 0,
      timestamp:            new Date().toISOString(),
    });
    await query(`SELECT pg_notify('pharmsub_alerts', $1)`, [payload]).catch(() => {});

    return {
      warehouse_name:      warehouseName,
      newly_expired_count: newlyExpired.length,
      near_expiry_count:   nearExpiryCount ?? 0,
      low_stock_count:     lowStockCount ?? 0,
      snapshot_count:      snapshotCount ?? 0,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ── ตรวจและรัน cut-off ที่ถึงเวลา ────────────────────────────────────────────
async function checkAndExecuteCutOff(): Promise<void> {
  const { month, day, hour, minute } = getThaiTime();

  const { rows } = await query(
    `SELECT mcp.med_period_id, mcp.sub_warehouse_id, sw.name AS warehouse_name
     FROM   ${SCHEMA}.med_cut_off_period mcp
     JOIN   ${SCHEMA}.sub_warehouse sw ON sw.sub_warehouse_id = mcp.sub_warehouse_id
     WHERE  mcp.is_active     = true
       AND  mcp.period_month  = $1
       AND  mcp.period_day    = $2
       AND  mcp.period_time_h = $3
       AND  mcp.period_time_m = $4
       AND  (
         mcp.last_executed_at IS NULL
         OR mcp.last_executed_at < NOW() - INTERVAL '50 minutes'
       )`,
    [month, day, hour, minute]
  );

  for (const row of rows) {
    // อัปเดต timestamp ก่อนรัน เพื่อป้องกัน double-trigger ถ้า logic ใช้เวลานาน
    await query(
      `UPDATE ${SCHEMA}.med_cut_off_period
       SET last_executed_at = NOW(), updated_at = NOW()
       WHERE med_period_id = $1`,
      [row.med_period_id]
    );

    try {
      const summary = await runCutOffLogic(row.sub_warehouse_id, row.warehouse_name);
      console.log(
        `[scheduler] ✅ ตัดรอบ: ${row.warehouse_name} ` +
        `| หมดอายุ=${summary.newly_expired_count} ` +
        `| ใกล้หมดอายุ=${summary.near_expiry_count} ` +
        `| สต็อกต่ำ=${summary.low_stock_count} ` +
        `| snapshot=${summary.snapshot_count} รายการ`
      );
    } catch (err) {
      console.error(`[scheduler] ❌ ตัดรอบ ${row.warehouse_name} ล้มเหลว:`, (err as Error).message);
    }
  }
}

// ── เริ่ม scheduler (รันทุก 1 นาที) ─────────────────────────────────────────
export function startScheduler(): void {
  checkAndExecuteCutOff().catch((err) =>
    console.error('[scheduler] error on startup check:', err)
  );

  setInterval(() => {
    checkAndExecuteCutOff().catch((err) =>
      console.error('[scheduler] error:', err)
    );
    query(`DELETE FROM ${SCHEMA}.sessions WHERE expires_at < NOW()`).catch(() => {});
  }, 60_000);

  console.log('[scheduler] 🕐 cut-off scheduler started (checks every 1 min, Thai time UTC+7)');
}
