import { Request, Response, NextFunction } from 'express';
import { query, pool, SCHEMA, resolveUserId } from '../db/pool';
import { AppError } from '../middleware/errorHandler';

// ── Helper: get current stock (with row lock) ─────────────────────────────────
async function getCurrentStock(med_sid: number, client: any): Promise<number> {
  const { rows } = await client.query(
    `SELECT med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`,
    [med_sid]
  );
  if (!rows.length) throw new AppError('ไม่พบยาใน subwarehouse', 404);
  return parseInt(rows[0].med_quantity);
}

// ── Helper: recalc med_subwarehouse.med_quantity from lot SUM ─────────────────
export async function recalcTotal(med_sid: number, client: any): Promise<number> {
  const { rows } = await client.query(
    `UPDATE ${SCHEMA}.med_subwarehouse
     SET med_quantity = COALESCE(
       (SELECT SUM(quantity) FROM ${SCHEMA}.med_stock_lots WHERE med_sid = $1), 0
     ),
     exp_date = (
       SELECT MIN(exp_date) FROM ${SCHEMA}.med_stock_lots
       WHERE med_sid = $1 AND exp_date IS NOT NULL
     ),
     updated_at = NOW()
     WHERE med_sid = $1
     RETURNING med_quantity`,
    [med_sid]
  );
  return parseInt(rows[0].med_quantity);
}

// ── Helper: deduct FEFO across lots ──────────────────────────────────────────
export async function deductFefo(
  med_sid: number, needed: number, client: any
): Promise<{ lot_number: string | null; exp_date: string | null; taken: number }[]> {
  const { rows: lots } = await client.query(
    `SELECT lot_id, lot_number, quantity, exp_date
     FROM ${SCHEMA}.med_stock_lots
     WHERE med_sid = $1
     ORDER BY exp_date ASC NULLS LAST, lot_id ASC
     FOR UPDATE`,
    [med_sid]
  );

  let remaining = needed;
  const consumed: { lot_number: string | null; exp_date: string | null; taken: number }[] = [];

  for (const lot of lots) {
    if (remaining <= 0) break;
    const take    = Math.min(remaining, lot.quantity);
    const newQty  = lot.quantity - take;
    if (newQty === 0) {
      await client.query(`DELETE FROM ${SCHEMA}.med_stock_lots WHERE lot_id = $1`, [lot.lot_id]);
    } else {
      await client.query(`UPDATE ${SCHEMA}.med_stock_lots SET quantity = $1 WHERE lot_id = $2`, [newQty, lot.lot_id]);
    }
    consumed.push({ lot_number: lot.lot_number, exp_date: lot.exp_date, taken: take });
    remaining -= take;
  }

  if (remaining > 0) {
    throw new AppError(`สต็อก lot ไม่เพียงพอ (ขาด ${remaining} หน่วย)`, 400);
  }
  return consumed;
}

// ── Helper: record transaction ────────────────────────────────────────────────
async function recordTx(client: any, data: {
  med_sid: number; med_id: number; tx_type: string;
  quantity: number; balance_before: number; balance_after: number;
  lot_number?: string; expiry_date?: string; mfg_date?: string;
  reference_no?: string; prescription_no?: string; ward_from?: string;
  performed_by?: string | null; note?: string;
  approval_status?: string; source_type?: string;
}) {
  const { rows } = await client.query(
    `INSERT INTO ${SCHEMA}.stock_transactions
       (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
        lot_number, expiry_date, mfg_date, reference_no, prescription_no,
        ward_from, performed_by, note, approval_status, source_type)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
     RETURNING *`,
    [data.med_sid, data.med_id, data.tx_type, data.quantity,
     data.balance_before, data.balance_after,
     data.lot_number, data.expiry_date, data.mfg_date ?? null,
     data.reference_no, data.prescription_no, data.ward_from,
     data.performed_by, data.note,
     data.approval_status ?? 'approved', data.source_type ?? null]
  );
  return rows[0];
}

// ── Helper: process one stock-in item within an active transaction ────────────
interface ReceiveItem {
  med_sid?: number; item_id?: string;
  quantity: number; lot_number?: string; expiry_date?: string; mfg_date?: string;
  note?: string; drug_code?: string; image_url?: string;
  med_name?: string; med_generic_name?: string; med_counting_unit?: string;
  med_medical_category?: string; packaging_type?: string;
  min_quantity?: number; max_quantity?: number;
}
interface ReceiveShared {
  reference_no?: string; performed_by?: number | null; source_type: string;
}

async function processOneReceive(client: any, item: ReceiveItem, shared: ReceiveShared) {
  const {
    med_sid, item_id, quantity, lot_number, expiry_date, mfg_date,
    note, drug_code, image_url, med_name, med_generic_name,
    med_counting_unit, med_medical_category, packaging_type,
    min_quantity: min_qty, max_quantity: max_qty,
  } = item;
  const { reference_no, performed_by, source_type } = shared;

  if (!med_sid && !item_id)
    throw new AppError('ต้องระบุ med_sid หรือ item_id', 400);
  if (!quantity || quantity <= 0)
    throw new AppError('quantity (>0) จำเป็น', 400);

  if (expiry_date) {
    const exp = new Date(expiry_date);
    const today = new Date(); today.setHours(0, 0, 0, 0);
    if (exp < today)
      throw new AppError(`ไม่สามารถรับยาที่หมดอายุแล้วเข้าคลัง (item_id: ${item_id ?? med_sid})`, 400);
  }

  // lookup: item_id → med_table.main_item_id → med_id → med_subwarehouse
  let resolvedMedId: number | null = null;
  if (item_id) {
    const { rows: mtRows } = await client.query(
      `SELECT med_id FROM ${SCHEMA}.med_table WHERE main_item_id = $1`,
      [item_id]
    );
    if (!mtRows.length)
      throw new AppError(`ไม่พบยาในทะเบียนยาหลัก (item_id: ${item_id})`, 404);
    resolvedMedId = mtRows[0].med_id;
  }

  let { rows: drugRows } = await (
    resolvedMedId
      ? client.query(
          `SELECT med_sid, med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_id = $1 FOR UPDATE`,
          [resolvedMedId]
        )
      : client.query(
          `SELECT med_sid, med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`,
          [med_sid]
        )
  );

  // Auto-create: ยาไม่มีใน subwarehouse → สร้างอัตโนมัติ
  let wasAutoCreated = false;
  if (!drugRows.length) {
    let medId: number;
    if (resolvedMedId) {
      medId = resolvedMedId;
    } else {
      if (!med_name) throw new AppError('ไม่พบยาในระบบ — กรุณาส่ง med_name เพื่อสร้างยาใหม่อัตโนมัติ', 404);
      const { rows: mtRows } = await client.query(
        `SELECT med_id FROM ${SCHEMA}.med_table WHERE med_name = $1 LIMIT 1`,
        [med_name]
      );
      if (mtRows.length) {
        medId = mtRows[0].med_id;
      } else {
        const { rows: newMt } = await client.query(
          `INSERT INTO ${SCHEMA}.med_table
             (med_name, med_generic_name, med_counting_unit, med_medical_category)
           VALUES ($1, $2, $3, $4) RETURNING med_id`,
          [med_name, med_generic_name || null,
           med_counting_unit || 'หน่วย', med_medical_category || null]
        );
        medId = newMt[0].med_id;
      }
    }
    // auto-create: ดึง med_showname / ราคา จาก med_table ผ่าน SELECT เพื่อไม่ให้ field ว่าง
    const { rows: newSub } = await client.query(
      `INSERT INTO ${SCHEMA}.med_subwarehouse
         (med_id, drug_code, image_url, packaging_type, min_quantity, max_quantity, med_quantity,
          med_showname, med_showname_eng, cost_price, unit_price)
       SELECT
         $1, $2, $3, $4, $5, $6, 0,
         COALESCE(mt.med_marketing_name, mt.med_thai_name, mt.med_name),
         mt.med_name,
         mt.med_cost_price::numeric,
         mt.med_selling_price::numeric
       FROM ${SCHEMA}.med_table mt WHERE mt.med_id = $1
       RETURNING med_sid, med_id, med_quantity`,
      [medId, drug_code || null, image_url || null,
       packaging_type || 'เม็ด', min_qty || null, max_qty || null]
    );
    drugRows = newSub;
    wasAutoCreated = true;
  }

  const drug      = drugRows[0];
  const balBefore = parseInt(drug.med_quantity);

  // Lot deduplication
  let newLotId: number;
  if (lot_number) {
    const { rows: existingLot } = await client.query(
      `SELECT lot_id FROM ${SCHEMA}.med_stock_lots
       WHERE med_sid = $1 AND lot_number = $2 AND status = 'active' LIMIT 1`,
      [drug.med_sid, lot_number]
    );
    if (existingLot.length) {
      await client.query(
        `UPDATE ${SCHEMA}.med_stock_lots SET quantity = quantity + $1, updated_at = NOW() WHERE lot_id = $2`,
        [parseInt(quantity), existingLot[0].lot_id]
      );
      newLotId = existingLot[0].lot_id;
    } else {
      const { rows: lotRows } = await client.query(
        `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, exp_date, mfg_date, note, status)
         VALUES ($1, $2, $3, $4::date, $5::date, $6, 'active') RETURNING lot_id`,
        [drug.med_sid, lot_number, parseInt(quantity), expiry_date || null, mfg_date || null, note || null]
      );
      newLotId = lotRows[0].lot_id;
    }
  } else {
    const { rows: lotRows } = await client.query(
      `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, exp_date, mfg_date, note, status)
       VALUES ($1, NULL, $2, $3::date, $4::date, $5, 'active') RETURNING lot_id`,
      [drug.med_sid, parseInt(quantity), expiry_date || null, mfg_date || null, note || null]
    );
    newLotId = lotRows[0].lot_id;
  }

  const balAfter = await recalcTotal(drug.med_sid, client);

  // sync: fill NULL fields จาก med_table อัตโนมัติ — ไม่ override ค่าที่มีอยู่แล้ว
  await client.query(
    `UPDATE ${SCHEMA}.med_subwarehouse ms
     SET
       med_showname     = COALESCE(ms.med_showname,     mt.med_marketing_name, mt.med_thai_name, mt.med_name),
       med_showname_eng = COALESCE(ms.med_showname_eng, mt.med_name),
       cost_price       = COALESCE(ms.cost_price,       mt.med_cost_price::numeric),
       unit_price       = COALESCE(ms.unit_price,       mt.med_selling_price::numeric),
       drug_code        = COALESCE(ms.drug_code,        $2),
       image_url        = COALESCE(ms.image_url,        $3),
       is_expired       = false,
       updated_at       = NOW()
     FROM ${SCHEMA}.med_table mt
     WHERE ms.med_sid = $1 AND mt.med_id = ms.med_id`,
    [drug.med_sid, drug_code || null, image_url || null]
  );

  const tx = await recordTx(client, {
    med_sid: drug.med_sid, med_id: drug.med_id,
    tx_type: 'in', quantity: parseInt(quantity),
    balance_before: balBefore, balance_after: balAfter,
    lot_number, expiry_date, mfg_date, reference_no,
    performed_by: performed_by ?? undefined, note,
    approval_status: 'approved', source_type,
  });

  await client.query(
    `UPDATE ${SCHEMA}.stock_transactions SET lot_id = $1 WHERE tx_id = $2`,
    [newLotId, tx.tx_id]
  );

  return { tx, lot_id: newLotId, balance_after: balAfter, auto_created: wasAutoCreated, med_sid: drug.med_sid };
}

// ── POST /stock/receive + /stock/from-main ────────────────────────────────────
// batch:  { reference_no, source_type, performed_by, items: [...] }
// single: { item_id, quantity, reference_no, ... }  (backward compatible)
// ทุกรายการอยู่ใน transaction เดียวกัน — รายการใดผิด rollback ทั้งหมด
export async function receiveStock(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const { items, reference_no, performed_by, source_type = 'main_warehouse', ...singleItem } = req.body;
    const isBatch = Array.isArray(items);

    if (isBatch && items.length === 0) throw new AppError('items ต้องมีอย่างน้อย 1 รายการ', 400);
    if (isBatch && items.length > 100) throw new AppError('รับได้สูงสุด 100 รายการต่อครั้ง', 400);

    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);

    const shared: ReceiveShared = { reference_no, performed_by: resolvedPerformer, source_type };
    const list: ReceiveItem[]   = isBatch ? items : [singleItem];

    const results = [];
    for (let i = 0; i < list.length; i++) {
      try {
        const result = await processOneReceive(client, list[i], shared);
        results.push({
          index: i,
          success: true,
          med_sid: result.med_sid,
          lot_id: result.lot_id,
          balance_after: result.balance_after,
          auto_created: result.auto_created,
          tx_id: result.tx.tx_id,
        });
      } catch (err: any) {
        throw new AppError(`รายการที่ ${i + 1}: ${err.message}`, err.statusCode ?? 400);
      }
    }

    await client.query('COMMIT');

    if (isBatch) {
      res.status(201).json({ total_received: results.length, results });
    } else {
      const r = results[0];
      res.status(201).json({
        tx_id: r.tx_id,
        lot_id: r.lot_id,
        balance_after: r.balance_after,
        auto_created: r.auto_created,
        new_med_sid: r.auto_created ? r.med_sid : undefined,
      });
    }
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── GET /stock/transactions ───────────────────────────────────────────────────
export async function getTransactions(req: Request, res: Response, next: NextFunction) {
  try {
    const {
      med_sid, tx_type, date_from, date_to,
      page = 1, limit = 50
    } = req.query;

    const params: any[] = [];
    let where = 'WHERE 1=1';
    let p = 1;

    if (med_sid) { where += ` AND st.med_sid = $${p}`; params.push(Number(med_sid)); p++; }
    if (tx_type) { where += ` AND st.tx_type = $${p}`; params.push(tx_type); p++; }
    if (date_from) { where += ` AND st.created_at >= $${p}`; params.push(date_from); p++; }
    if (date_to)   { where += ` AND st.created_at < $${p}::date + 1`; params.push(date_to); p++; }

    const offset = (Number(page) - 1) * Number(limit);

    const { rows } = await query(
      `SELECT
         st.*,
         ms.med_showname, ms.med_showname_eng,
         mt.med_name, mt.med_generic_name,
         COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS performed_by_name
       FROM ${SCHEMA}.stock_transactions st
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = st.med_id
       LEFT JOIN public.profiles pu ON pu.id = st.performed_by
       LEFT JOIN auth.users     au ON au.id = st.performed_by
       ${where}
       ORDER BY st.created_at DESC
       LIMIT $${p} OFFSET $${p + 1}`,
      [...params, Number(limit), offset]
    );

    const countRes = await query(
      `SELECT COUNT(*) AS total
       FROM ${SCHEMA}.stock_transactions st
       ${where}`, params
    );

    res.json({ data: rows, total: parseInt(countRes.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── POST /stock/in — สร้างคำขอรับยาเข้าคลัง (รอการอนุมัติ) ──────────────────
export async function stockIn(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const {
      med_sid, quantity, lot_number, expiry_date, mfg_date,
      reference_no, performed_by, note,
      source_type,   // 'main_warehouse' | 'supplier' | undefined
    } = req.body;

    if (!med_sid || !quantity || quantity <= 0)
      throw new AppError('med_sid และ quantity (>0) จำเป็น', 400);

    if (expiry_date) {
      const exp = new Date(expiry_date);
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      if (exp < today)
        throw new AppError('ไม่สามารถรับยาที่หมดอายุแล้วเข้าคลังได้', 400);
    }

    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);

    const { rows: drugRows } = await client.query(
      `SELECT med_sid, med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`,
      [med_sid]
    );
    if (!drugRows.length) throw new AppError('ไม่พบยาใน subwarehouse', 404);

    const drug      = drugRows[0];
    const balBefore = parseInt(drug.med_quantity);

    // สร้าง transaction สถานะ pending — ยังไม่แตะสต็อก รอคลังหลักอนุมัติ
    const tx = await recordTx(client, {
      med_sid: drug.med_sid, med_id: drug.med_id,
      tx_type: 'in', quantity: parseInt(quantity),
      balance_before: balBefore, balance_after: balBefore,
      lot_number, expiry_date, mfg_date, reference_no,
      performed_by: resolvedPerformer ?? undefined, note,
      approval_status: 'pending', source_type,
    });

    await client.query('COMMIT');
    res.status(201).json(tx);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── GET /stock/pending-in — รายการรออนุมัติ (สำหรับคลังหลัก) ────────────────
export async function getPendingStockIn(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(
      `SELECT
         st.*,
         ms.med_showname, ms.med_showname_eng,
         mt.med_name, mt.med_generic_name,
         COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS performed_by_name
       FROM ${SCHEMA}.stock_transactions st
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = st.med_id
       LEFT JOIN public.profiles pu ON pu.id = st.performed_by
       LEFT JOIN auth.users     au ON au.id = st.performed_by
       WHERE st.tx_type = 'in' AND st.approval_status = 'pending'
       ORDER BY st.created_at ASC`
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ── PATCH /stock/:tx_id/approve — อนุมัติและเพิ่มสต็อก ──────────────────────
export async function approveStockIn(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const { tx_id } = req.params;
    const { approved_by } = req.body;
    const resolvedApprover = approved_by != null
      ? await resolveUserId(approved_by)
      : ((req as any).currentUser?.id ?? null);

    const { rows: txRows } = await client.query(
      `SELECT * FROM ${SCHEMA}.stock_transactions WHERE tx_id = $1 FOR UPDATE`,
      [tx_id]
    );
    if (!txRows.length) throw new AppError('ไม่พบรายการ', 404);
    const tx = txRows[0];
    if (tx.approval_status !== 'pending')
      throw new AppError('รายการนี้ไม่ได้อยู่ในสถานะรออนุมัติ', 400);

    // เพิ่ม lot และอัปเดตสต็อก
    const { rows: lotRows } = await client.query(
      `INSERT INTO ${SCHEMA}.med_stock_lots
         (med_sid, lot_number, quantity, exp_date, mfg_date, note, status)
       VALUES ($1, $2, $3, $4::date, $5::date, $6, 'active')
       RETURNING lot_id`,
      [tx.med_sid, tx.lot_number || null, tx.quantity,
       tx.expiry_date || null, tx.mfg_date || null, tx.note || null]
    );
    const newLotId = lotRows[0].lot_id;
    const balAfter = await recalcTotal(parseInt(tx.med_sid), client);
    await client.query(
      `UPDATE ${SCHEMA}.med_subwarehouse SET is_expired = false WHERE med_sid = $1`,
      [tx.med_sid]
    );

    // อัปเดต transaction — บันทึก lot_id ที่สร้าง เพื่อ trace ย้อนกลับได้
    const { rows: updated } = await client.query(
      `UPDATE ${SCHEMA}.stock_transactions
       SET approval_status = 'approved', balance_after = $1,
           approved_by = $2, approved_at = NOW(), lot_id = $3
       WHERE tx_id = $4
       RETURNING *`,
      [balAfter, resolvedApprover, newLotId, tx_id]
    );

    await client.query('COMMIT');
    res.json(updated[0]);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── PATCH /stock/:tx_id/reject — ปฏิเสธคำขอ ─────────────────────────────────
export async function rejectStockIn(req: Request, res: Response, next: NextFunction) {
  try {
    const { tx_id } = req.params;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.stock_transactions
       SET approval_status = 'rejected', approved_at = NOW()
       WHERE tx_id = $1 AND approval_status = 'pending'
       RETURNING *`,
      [tx_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการหรือไม่ใช่สถานะรออนุมัติ', 400);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── POST /stock/adjust — ปรับสต็อก ───────────────────────────────────────────
export async function adjustStock(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const { med_sid, new_quantity, performed_by, note } = req.body;
    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);
    if (med_sid === undefined || new_quantity === undefined)
      throw new AppError('med_sid และ new_quantity จำเป็น', 400);
    if (new_quantity < 0) throw new AppError('จำนวนต้องไม่น้อยกว่า 0', 400);

    const balBefore = await getCurrentStock(parseInt(med_sid), client);
    const diff = parseInt(new_quantity) - balBefore;

    const { rows: drugRows } = await client.query(
      `SELECT med_id FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`, [med_sid]
    );

    if (diff > 0) {
      // เพิ่มสต็อก — สร้าง lot ปรับยอด
      await client.query(
        `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, note)
         VALUES ($1, 'ADJ-' || to_char(NOW(), 'YYYYMMDD-HH24MISS'), $2, $3)`,
        [med_sid, diff, note || 'ปรับสต็อกเพิ่ม']
      );
    } else if (diff < 0) {
      // ลดสต็อก — ตัดจาก lots ตาม FEFO
      await deductFefo(parseInt(med_sid), Math.abs(diff), client);
    }

    const balAfter = await recalcTotal(parseInt(med_sid), client);

    const tx = await recordTx(client, {
      med_sid: parseInt(med_sid), med_id: drugRows[0].med_id,
      tx_type: 'adjust', quantity: diff,
      balance_before: balBefore, balance_after: balAfter,
      performed_by: resolvedPerformer ?? undefined, note
    });

    await client.query('COMMIT');
    res.status(201).json(tx);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── POST /stock/return — รับยาคืน ────────────────────────────────────────────
export async function returnStock(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const { med_sid, quantity, ward_from, reference_no, performed_by, note } = req.body;
    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);
    if (!med_sid || !quantity || quantity <= 0)
      throw new AppError('med_sid และ quantity (>0) จำเป็น', 400);

    const balBefore = await getCurrentStock(parseInt(med_sid), client);
    const { rows: drugRows } = await client.query(
      `SELECT med_id FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`, [med_sid]
    );

    // สร้าง lot คืนยา (ไม่มี exp_date — เรียงท้ายสุดใน FEFO)
    await client.query(
      `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, note)
       VALUES ($1, 'RET-' || to_char(NOW(), 'YYYYMMDD-HH24MISS'), $2, $3)`,
      [med_sid, parseInt(quantity), note || 'คืนยา']
    );

    const balAfter = await recalcTotal(parseInt(med_sid), client);

    const tx = await recordTx(client, {
      med_sid: parseInt(med_sid), med_id: drugRows[0].med_id,
      tx_type: 'return', quantity: parseInt(quantity),
      balance_before: balBefore, balance_after: balAfter,
      ward_from, reference_no, performed_by: resolvedPerformer ?? undefined, note
    });

    await client.query('COMMIT');
    res.status(201).json(tx);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── POST /stock/expired — ตัดยาหมดอายุออก ───────────────────────────────────
export async function markExpired(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL search_path TO ${SCHEMA}, public`);

    const { med_sid, quantity, lot_number, performed_by, note } = req.body;
    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);
    if (!med_sid || !quantity || quantity <= 0)
      throw new AppError('med_sid และ quantity (>0) จำเป็น', 400);

    const balBefore = await getCurrentStock(parseInt(med_sid), client);
    if (parseInt(quantity) > balBefore)
      throw new AppError('จำนวนมากกว่าสต็อกปัจจุบัน', 400);

    const { rows: drugRows } = await client.query(
      `SELECT med_id FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`, [med_sid]
    );

    if (lot_number) {
      // ตัดจาก lot ที่ระบุโดยตรง
      const { rows: targetLot } = await client.query(
        `SELECT lot_id, quantity FROM ${SCHEMA}.med_stock_lots
         WHERE med_sid = $1 AND lot_number = $2 FOR UPDATE`,
        [med_sid, lot_number]
      );
      if (!targetLot.length) throw new AppError('ไม่พบ lot นี้', 404);
      if (parseInt(quantity) > targetLot[0].quantity)
        throw new AppError('จำนวนมากกว่าที่มีใน lot นี้', 400);
      const newQty = targetLot[0].quantity - parseInt(quantity);
      if (newQty === 0) {
        await client.query(`DELETE FROM ${SCHEMA}.med_stock_lots WHERE lot_id = $1`, [targetLot[0].lot_id]);
      } else {
        await client.query(`UPDATE ${SCHEMA}.med_stock_lots SET quantity = $1 WHERE lot_id = $2`, [newQty, targetLot[0].lot_id]);
      }
    } else {
      // FEFO
      await deductFefo(parseInt(med_sid), parseInt(quantity), client);
    }

    const balAfter = await recalcTotal(parseInt(med_sid), client);

    // อัปเดต is_expired flag
    await client.query(
      `UPDATE ${SCHEMA}.med_subwarehouse SET is_expired = $1, updated_at = NOW() WHERE med_sid = $2`,
      [balAfter === 0, med_sid]
    );

    // บันทึกใน expired_medicines
    await client.query(
      `INSERT INTO ${SCHEMA}.expired_medicines (med_sid, med_id, status)
       VALUES ($1, $2, 'disposed')`,
      [med_sid, drugRows[0].med_id]
    );

    const tx = await recordTx(client, {
      med_sid: parseInt(med_sid), med_id: drugRows[0].med_id,
      tx_type: 'expired', quantity: parseInt(quantity),
      balance_before: balBefore, balance_after: balAfter,
      lot_number, performed_by: resolvedPerformer ?? undefined, note
    });

    await client.query('COMMIT');
    res.status(201).json(tx);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ── GET /stock/summary — สรุปรายวัน ──────────────────────────────────────────
export async function getStockSummary(req: Request, res: Response, next: NextFunction) {
  try {
    const days = parseInt(req.query.days as string) || 14;
    const { rows } = await query(
      `SELECT
         DATE(created_at) AS date,
         SUM(CASE WHEN tx_type = 'in'     THEN quantity ELSE 0 END) AS stock_in,
         SUM(CASE WHEN tx_type = 'out'    THEN ABS(quantity) ELSE 0 END) AS stock_out,
         SUM(CASE WHEN tx_type = 'return' THEN quantity ELSE 0 END) AS stock_return,
         SUM(CASE WHEN tx_type = 'expired' THEN ABS(quantity) ELSE 0 END) AS stock_expired
       FROM ${SCHEMA}.stock_transactions
       WHERE created_at >= NOW() - ($1 || ' days')::interval
       GROUP BY DATE(created_at)
       ORDER BY date ASC`,
      [days]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ── GET /stock/lots-report ─────────────────────────────────────────────────────
// type: 'expired' | 'near_expiry' | 'low_stock' | 'all' (default: all)
export async function getLotsReport(req: Request, res: Response, next: NextFunction) {
  try {
    const type = (req.query.type as string) || 'all';
    const days = parseInt(req.query.days as string) || 90; // near_expiry threshold

    let lotWhere = 'TRUE';
    if (type === 'expired') {
      lotWhere = `l.exp_date IS NOT NULL AND l.exp_date < CURRENT_DATE`;
    } else if (type === 'near_expiry') {
      lotWhere = `l.exp_date IS NOT NULL AND l.exp_date >= CURRENT_DATE AND l.exp_date <= CURRENT_DATE + ($1 || ' days')::interval`;
    } else if (type === 'low_stock') {
      // Low stock: subwarehouse med_quantity <= min_quantity
      const { rows } = await query(
        `SELECT
           ms.med_sid, ms.med_quantity AS current_stock,
           ms.min_quantity, ms.packaging_type,
           COALESCE(ms.med_showname, mt.med_name) AS drug_name,
           mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit,
           mt.med_medical_category AS category,
           ms.location
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         WHERE ms.min_quantity IS NOT NULL
           AND ms.med_quantity <= ms.min_quantity
         ORDER BY (ms.med_quantity::float / NULLIF(ms.min_quantity, 0)) ASC`
      );
      return res.json({ type: 'low_stock', data: rows });
    }

    const params: any[] = type === 'near_expiry' ? [days] : [];
    const { rows } = await query(
      `SELECT
         l.lot_id, l.lot_number, l.quantity, l.exp_date, l.mfg_date, l.received_at, l.note,
         ms.med_sid, ms.packaging_type, ms.location,
         COALESCE(ms.med_showname, mt.med_name) AS drug_name,
         mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit,
         mt.med_medical_category AS category,
         CASE
           WHEN l.exp_date IS NULL THEN 'no_expiry'
           WHEN l.exp_date < CURRENT_DATE THEN 'expired'
           WHEN l.exp_date <= CURRENT_DATE + '30 days'::interval THEN 'critical'
           WHEN l.exp_date <= CURRENT_DATE + '90 days'::interval THEN 'warning'
           ELSE 'ok'
         END AS exp_status,
         (l.exp_date - CURRENT_DATE) AS days_to_expiry
       FROM ${SCHEMA}.med_stock_lots l
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = l.med_sid
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       WHERE l.quantity > 0 AND ${lotWhere}
       ORDER BY l.exp_date ASC NULLS LAST, l.lot_id ASC`,
      params
    );

    res.json({ type, days, data: rows });
  } catch (err) { next(err); }
}

// ── GET /stock/requisitions — คำขอเบิกยาจากห้องจ่ายยา (department_id=7) ────────
export async function getRequisitions(req: Request, res: Response, next: NextFunction) {
  try {
    const { status, page = 1, limit = 30 } = req.query;
    const offset = (Number(page) - 1) * Number(limit);
    const DEPT_ID = 7; // ห้องจ่ายยา

    const params: any[] = [DEPT_ID];
    let whereExtra = '';
    if (status && status !== 'all') {
      whereExtra = ` AND rh.status = $${params.length + 1}`;
      params.push(String(status).toUpperCase());
    }

    const { rows } = await query(
      `SELECT
         rh.id,
         rh.doc_no,
         rh.type,
         rh.status,
         rh.request_date,
         rh.due_date,
         rh.note,
         rh.created_at,
         rh.updated_at,
         COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS requester_name,
         COUNT(ri.id)::integer AS item_count,
         COALESCE(SUM(ri.req_qty), 0)::integer AS total_req_qty,
         COALESCE(json_agg(
           json_build_object(
             'id',          ri.id,
             'item_name',   COALESCE(ms.med_showname, inv.name),
             'item_code',   inv.code,
             'req_qty',     ri.req_qty,
             'approved_qty',ri.approved_qty,
             'issued_qty',  ri.issued_qty,
             'note',        ri.note
           ) ORDER BY ri.id
         ) FILTER (WHERE ri.id IS NOT NULL), '[]') AS items
       FROM inventory.requisition_header rh
       LEFT JOIN public.profiles pu      ON pu.id = rh.requester_id
       LEFT JOIN auth.users     au      ON au.id = rh.requester_id
       LEFT JOIN inventory.requisition_item ri ON ri.header_id = rh.id
       LEFT JOIN inventory.items inv           ON inv.id = ri.item_id
       LEFT JOIN ${SCHEMA}.med_table ms        ON ms.main_item_id = ri.item_id
       WHERE rh.department_id = $1
       ${whereExtra}
       GROUP BY rh.id, pu.firstname_th, pu.lastname_th, au.email
       ORDER BY rh.created_at DESC
       LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, Number(limit), offset]
    );

    const { rows: countRows } = await query(
      `SELECT COUNT(*) AS total
       FROM inventory.requisition_header
       WHERE department_id = $1
       ${status && status !== 'all' ? `AND status = $2` : ''}`,
      status && status !== 'all' ? [DEPT_ID, String(status).toUpperCase()] : [DEPT_ID]
    );

    res.json({ data: rows, total: parseInt(countRows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}
