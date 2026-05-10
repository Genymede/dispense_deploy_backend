/**
 * scheduler.ts — ตรวจ cut-off period ทุก 1 นาที แล้วรันอัตโนมัติเมื่อถึงเวลา
 */
import { pool, query, SCHEMA } from './db/pool';

export interface LotDetail {
  lot_id: number;
  lot_number: string;
  med_sid: number;
  med_showname: string;
  exp_date: string;
  quantity: number;
  days_remaining?: number;
}

export interface SnapshotDetail {
  med_showname: string;
  quantity: number;
  unit: string;
  exp_date?: string; // เพิ่มมาเพื่อดูว่าระบบเห็นวันหมดอายุเป็นอะไร
}

export interface CutOffSummary {
  warehouse_name: string;
  newly_expired_count: number;
  near_expiry_count: number;
  low_stock_count: number;
  snapshot_count: number;
  expired_lots: LotDetail[];
  near_expiry_lots: LotDetail[];
  snapshot_details: SnapshotDetail[];
}

// ── logic จริงของการตัดรอบ ────────────────────────────────────────────────
export async function runCutOffLogic(warehouseId: number, warehouseName: string): Promise<CutOffSummary> {
  const client = await pool.connect();
  // บังคับใช้ SCHEMA จาก config หรือถ้าไม่มีให้ใช้ dispense_02
  const schemaName = SCHEMA || 'dispense_02';
  
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${schemaName}, public`);

    // 1. ค้นหายาทุกรายการในคลังนี้เพื่อทำ Snapshot และตรวจสอบข้อมูลดิบ
    // ผมดึง exp_date มาด้วยเพื่อวิเคราะห์ว่าทำไม Logic ถึงไม่นับ
    const { rows: allItems } = await client.query(
      `SELECT med_showname, med_quantity as quantity, exp_date, 'หน่วย' as unit
       FROM med_subwarehouse
       WHERE sub_warehouse_id = $1
       ORDER BY med_showname ASC`,
      [warehouseId]
    );

    // 2. ตรวจสอบยาหมดอายุจาก Lot จริง
    // ผมเอาเงื่อนไข l.status ออกทั้งหมดเพื่อ "กวาด" ข้อมูลมาให้ครบที่สุด
    const { rows: expiredLots } = await client.query<LotDetail>(
      `SELECT l.lot_id, l.lot_number, l.med_sid, ms.med_showname, l.exp_date, l.quantity
       FROM med_stock_lots l
       JOIN med_subwarehouse ms ON l.med_sid = ms.med_sid
       WHERE ms.sub_warehouse_id = $1
         AND l.exp_date < CURRENT_DATE
         AND l.quantity > 0`,
      [warehouseId]
    );

    // 3. ตรวจสอบยาใกล้หมดอายุ (ภายใน 120 วัน - ขยายเผื่อไว้เลยครับ)
    const { rows: nearExpiryLots } = await client.query<LotDetail>(
      `SELECT l.lot_id, l.lot_number, l.med_sid, ms.med_showname, l.exp_date, l.quantity,
              (l.exp_date - CURRENT_DATE)::int AS days_remaining
       FROM med_stock_lots l
       JOIN med_subwarehouse ms ON l.med_sid = ms.med_sid
       WHERE ms.sub_warehouse_id = $1
         AND l.exp_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '120 days'
         AND l.quantity > 0
       ORDER BY l.exp_date ASC`,
      [warehouseId]
    );

    // 4. ตรวจสอบสต็อกต่ำ (Low Stock) - ตัดเงื่อนไข is_expired ออกเพื่อดูว่าระบบมองเห็นบ้างไหม
    const { rows: lowStockItems } = await client.query(
      `SELECT med_sid FROM med_subwarehouse
       WHERE sub_warehouse_id = $1 
         AND min_quantity IS NOT NULL 
         AND med_quantity < min_quantity`,
      [warehouseId]
    );

    await client.query('COMMIT');

    // จัดเตรียม Snapshot details
    const snapshotDetails: SnapshotDetail[] = allItems.map(i => ({
      med_showname: i.med_showname,
      quantity: i.quantity,
      unit: i.unit,
      exp_date: i.exp_date ? new Date(i.exp_date).toLocaleDateString('th-TH') : 'ไม่มีวันหมดอายุ'
    }));

    return {
      warehouse_name:      warehouseName,
      newly_expired_count: expiredLots.length,
      near_expiry_count:   nearExpiryLots.length,
      low_stock_count:     lowStockItems.length,
      snapshot_count:      allItems.length,
      expired_lots:        expiredLots,
      near_expiry_lots:    nearExpiryLots,
      snapshot_details:    snapshotDetails,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ── คำนวณ scheduled datetime ของรอบนั้นในปัจจุบัน ────────────────────────────
export function getScheduledTime(period: any, now: Date): Date {
  const year = now.getFullYear();
  // monthly = เดือนปัจจุบัน, yearly = เดือนที่กำหนด (1-indexed → 0-indexed)
  const month = period.frequency === 'yearly'
    ? period.period_month - 1
    : now.getMonth();

  // clamp วันที่ กรณี period_day=31 แต่เดือนนั้นมีแค่ 28-30 วัน
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const day = Math.min(period.period_day, daysInMonth);

  return new Date(year, month, day, period.period_time_h, period.period_time_m, 0, 0);
}

// ── ตรวจและรัน cut-off ที่ถึงเวลา ────────────────────────────────────────────
async function checkAndExecuteCutOff(): Promise<void> {
  try {
    const { rows: periods } = await query(
      `SELECT mcp.*, sw.name AS warehouse_name
       FROM ${SCHEMA}.med_cut_off_period mcp
       JOIN ${SCHEMA}.sub_warehouse sw ON sw.sub_warehouse_id = mcp.sub_warehouse_id
       WHERE mcp.is_active = true`
    );

    const now = new Date();

    for (const period of periods) {
      const scheduledAt = getScheduledTime(period, now);

      // ถึงเวลาแล้ว AND ยังไม่เคยรันในรอบนี้
      const alreadyRan = period.last_executed_at &&
        new Date(period.last_executed_at) >= scheduledAt;

      if (now < scheduledAt || alreadyRan) continue;

      try {
        await runCutOffLogic(period.sub_warehouse_id, period.warehouse_name);
        await query(
          `UPDATE ${SCHEMA}.med_cut_off_period
           SET last_executed_at = NOW(), updated_at = NOW()
           WHERE med_period_id = $1`,
          [period.med_period_id]
        );
        console.log(
          `[Scheduler] cut-off ✓  warehouse="${period.warehouse_name}"` +
          `  period=${period.med_period_id}  at=${now.toISOString()}`
        );
      } catch (err) {
        console.error(
          `[Scheduler] cut-off ✗  period=${period.med_period_id}`, err
        );
      }
    }
  } catch (err) {
    console.error('[Scheduler] checkAndExecuteCutOff error:', err);
  }
}

export function startScheduler(): void {
  console.log('[Scheduler] started — checking every 60 s');
  // รันทันทีครั้งแรก แล้วทุก 1 นาที
  checkAndExecuteCutOff();
  setInterval(checkAndExecuteCutOff, 60_000);
}
