/**
 * scheduler.ts — ตรวจ cut-off period ทุก 1 นาที แล้วรันอัตโนมัติเมื่อถึงเวลา
 * ไม่ต้องติดตั้ง package เพิ่ม ใช้ setInterval ของ Node ล้วนๆ
 */
import { query, SCHEMA } from './db/pool';

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

// ── ตรวจและรัน cut-off ที่ถึงเวลา ────────────────────────────────────────────
async function checkAndExecuteCutOff(): Promise<void> {
  const { month, day, hour, minute } = getThaiTime();

  // หา cut-off ที่ตรงเวลาปัจจุบัน + ยังไม่เคยรันในนาทีนี้
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
    await query(
      `UPDATE ${SCHEMA}.med_cut_off_period
       SET last_executed_at = NOW(), updated_at = NOW()
       WHERE med_period_id = $1`,
      [row.med_period_id]
    );
    console.log(
      `[scheduler] ✅ ตัดรอบอัตโนมัติ: ${row.warehouse_name} ` +
      `(id=${row.med_period_id}) เวลา ${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')} ` +
      `วันที่ ${day}/${month}`
    );
  }
}

// ── เริ่ม scheduler (รันทุก 1 นาที) ─────────────────────────────────────────
export function startScheduler(): void {
  // รันครั้งแรกทันทีเมื่อ server start (กันกรณี restart ตอนใกล้เวลา)
  checkAndExecuteCutOff().catch((err) =>
    console.error('[scheduler] error on startup check:', err)
  );

  setInterval(() => {
    checkAndExecuteCutOff().catch((err) =>
      console.error('[scheduler] error:', err)
    );
    query(`DELETE FROM ${SCHEMA}.sessions WHERE expires_at < NOW()`).catch(() => {});
  }, 60_000); // ทุก 1 นาที

  console.log('[scheduler] 🕐 cut-off scheduler started (checks every 1 min, Thai time UTC+7)');
}
