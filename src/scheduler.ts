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

// ── ตรวจและรัน cut-off ที่ถึงเวลา ────────────────────────────────────────────
async function checkAndExecuteCutOff(): Promise<void> {
  // ... (ส่วนนี้คงเดิม)
}

export function startScheduler(): void {
  // ... (ส่วนนี้คงเดิม)
}
