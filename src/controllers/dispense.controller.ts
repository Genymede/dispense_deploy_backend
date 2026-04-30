import { Request, Response, NextFunction } from 'express';
import { query, pool, SCHEMA, resolveUserId, resolvePatientId, paginate } from '../db/pool';
import { AppError } from '../middleware/errorHandler';
import { nextQueueNumber } from './queue.controller';
import { deductFefo, recalcTotal } from './stock.controller';

// ── GET /dispense — รายการใบสั่งยา ───────────────────────────────────────────
export async function getPrescriptions(req: Request, res: Response, next: NextFunction) {
  try {
    const { status, ward, date_from, date_to, search, patient_id } = req.query;
    const { limit: lim, offset } = paginate(req.query.page, req.query.limit);

    const params: any[] = [];
    let where = "WHERE COALESCE(pr.source, 'counter') = 'counter'";
    let p = 1;

    // ถ้า status = dispensed ให้กรองด้วย dispensed_at (วันที่จ่ายจริง) ไม่ใช่ created_at
    const dateField = status === 'dispensed' ? 'pr.dispensed_at' : 'pr.created_at';

    if (patient_id) { where += ` AND pr.patient_id = $${p}`; params.push(Number(patient_id)); p++; }
    if (status) { where += ` AND pr.status = $${p}`; params.push(status); p++; }
    if (ward) { where += ` AND pr.ward = $${p}`; params.push(ward); p++; }
    if (date_from) { where += ` AND DATE(${dateField} AT TIME ZONE 'Asia/Bangkok') >= $${p}::date`; params.push(date_from); p++; }
    if (date_to) { where += ` AND DATE(${dateField} AT TIME ZONE 'Asia/Bangkok') <= $${p}::date`; params.push(date_to); p++; }
    if (search) {
      where += ` AND (pr.prescription_no ILIKE $${p} OR pa.first_name ILIKE $${p} OR pa.last_name ILIKE $${p} OR pa.hn_number ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }

    const { rows } = await query(
      `SELECT
         pr.*,
         pa.first_name, pa.last_name, pa.hn_number,
         pa.blood_group, pa.photo AS patient_photo,
         pa.treatment_right, pa.treatment_right_note,
         CONCAT(pa.first_name, ' ', pa.last_name) AS patient_name,
         COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
         COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name,
         (SELECT COUNT(*)
          FROM ${SCHEMA}.prescription_items pi
          WHERE pi.prescription_id = pr.prescription_id) AS item_count,
         (SELECT COALESCE(SUM(pi.quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)), 0)
          FROM   ${SCHEMA}.prescription_items pi
          JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = pi.med_sid
          JOIN   ${SCHEMA}.med_table mt        ON mt.med_id  = pi.med_id
          WHERE  pi.prescription_id = pr.prescription_id) AS total_cost
       FROM ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = pr.patient_id
       LEFT JOIN public.profiles pdoc  ON pdoc.id = pr.doctor_id
       LEFT JOIN auth.users     adoc  ON adoc.id = pr.doctor_id
       LEFT JOIN public.profiles pdisp ON pdisp.id = pr.dispensed_by
       LEFT JOIN auth.users     adisp ON adisp.id = pr.dispensed_by
       ${where}
       ORDER BY ${dateField} DESC NULLS LAST
       LIMIT $${p} OFFSET $${p + 1}`,
      [...params, lim, offset]
    );

    const countRes = await query(
      `SELECT COUNT(*) AS total
       FROM ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = pr.patient_id
       ${where}`, params
    );

    res.json({ data: rows, total: parseInt(countRes.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── GET /dispense/:id ─────────────────────────────────────────────────────────
export async function getPrescriptionById(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { rows } = await query(
      `SELECT
         pr.*,
         pa.first_name, pa.last_name, pa.hn_number,
         pa.blood_group, pa.photo AS patient_photo,
         pa.treatment_right, pa.treatment_right_note,
         CONCAT(pa.first_name, ' ', pa.last_name) AS patient_name,
         COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
         COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name
       FROM ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient pa    ON pa.patient_id = pr.patient_id
       LEFT JOIN public.profiles pdoc   ON pdoc.id = pr.doctor_id
       LEFT JOIN auth.users     adoc   ON adoc.id = pr.doctor_id
       LEFT JOIN public.profiles pdisp  ON pdisp.id = pr.dispensed_by
       LEFT JOIN auth.users     adisp  ON adisp.id = pr.dispensed_by
       WHERE pr.prescription_id = $1`, [id]
    );
    if (!rows.length) throw new AppError('ไม่พบใบสั่งยา', 404);

    // items
    const { rows: items } = await query(
      `SELECT
         pi.*,
         COALESCE(ms.unit_price, mt.med_selling_price, 0) AS unit_price,
         COALESCE(ms.unit_price, mt.med_selling_price, 0) AS current_unit_price,
         ms.med_showname, ms.med_showname_eng, ms.packaging_type,
         mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit,
         (pi.quantity::numeric * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS line_total
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = pi.med_sid
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
       WHERE pi.prescription_id = $1`, [id]
    );

    res.json({ ...rows[0], items });
  } catch (err) { next(err); }
}

// ── POST /dispense — สร้างใบสั่งยา ──────────────────────────────────────────
export async function createPrescription(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const {
      patient_id, doctor_id, ward, note,
      items // [{ med_sid, quantity, dose, frequency, route }]
    } = req.body;

    if (!items || !Array.isArray(items) || items.length === 0)
      throw new AppError('ต้องมีรายการยาอย่างน้อย 1 รายการ', 400);
    const resolvedPatient = await resolvePatientId(patient_id);
    const resolvedDoctor = doctor_id != null
      ? await resolveUserId(doctor_id)
      : ((req as any).currentUser?.id ?? null);

    // Generate prescription_no: RX-YYYYMMDD-NNNN
    const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const countRes = await client.query(
      `SELECT COUNT(*) AS cnt FROM ${SCHEMA}.prescriptions WHERE DATE(created_at) = CURRENT_DATE`
    );
    const seq = String(parseInt(countRes.rows[0].cnt) + 1).padStart(4, '0');
    const prescription_no = `RX-${dateStr}-${seq}`;

    const { rows: prRows } = await client.query(
      `INSERT INTO ${SCHEMA}.prescriptions
         (prescription_no, patient_id, doctor_id, ward, status, note)
       VALUES ($1,$2,$3,$4,'pending',$5)
       RETURNING *`,
      [prescription_no, resolvedPatient || null, resolvedDoctor || null, ward, note]
    );
    const prescription = prRows[0];

    // insert items
    for (const item of items) {
      if (!item.med_sid || !item.quantity || item.quantity <= 0)
        throw new AppError('รายการยาต้องมี med_sid และ quantity > 0', 400);

      // get med_id from med_sid
      const { rows: drugRows } = await client.query(
        `SELECT med_id FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`, [item.med_sid]
      );
      if (!drugRows.length) throw new AppError(`ไม่พบ med_sid: ${item.med_sid}`, 404);

      await client.query(
        `INSERT INTO ${SCHEMA}.prescription_items
           (prescription_id, med_sid, med_id, quantity, dose, frequency, route)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [prescription.prescription_id, item.med_sid, drugRows[0].med_id,
        item.quantity, item.dose, item.frequency, item.route]
      );
    }


    await client.query('COMMIT');
    res.status(201).json({ ...prescription, item_count: items.length });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) { }
    next(err);
  } finally { client.release(); }
}

// ── POST /dispense/:id/dispense — จ่ายยา ─────────────────────────────────────
export async function dispensePrescription(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { id } = req.params;
    const { dispensed_by, overdue_items } = req.body;
    // ถ้าไม่มี dispensed_by ใน body ให้ใช้ uid จาก JWT/session token แทน
    const resolvedDispenser = dispensed_by != null
      ? await resolveUserId(dispensed_by)
      : ((req as any).currentUser?.id ?? null);

    // overdue_items: [{ item_id, overdue_qty }] — จ่ายส่วนที่มีสต็อก ค้างจ่ายส่วนต่าง
    const overdueMap = new Map<number, number>();
    for (const o of (Array.isArray(overdue_items) ? overdue_items : [])) {
      overdueMap.set(Number(o.item_id), Number(o.overdue_qty));
    }

    // check prescription
    const { rows: prRows } = await client.query(
      `SELECT * FROM ${SCHEMA}.prescriptions WHERE prescription_id = $1 FOR UPDATE`, [id]
    );
    if (!prRows.length) throw new AppError('ไม่พบใบสั่งยา', 404);
    if (prRows[0].status !== 'pending')
      throw new AppError(`ไม่สามารถจ่ายยาได้ สถานะปัจจุบัน: ${prRows[0].status}`, 400);

    const pr = prRows[0];

    // get items
    const { rows: items } = await client.query(
      `SELECT pi.item_id, pi.med_sid, pi.med_id, pi.quantity, pi.frequency, pi.route
       FROM ${SCHEMA}.prescription_items pi WHERE pi.prescription_id = $1`, [id]
    );

    // deduct stock หรือ บันทึกยาค้างจ่าย ตามแต่ละ item
    for (const item of items) {

      // ── บันทึกยาค้างจ่าย (ทั้งหมดหรือบางส่วน) ──────────────────────────────
      if (overdueMap.has(item.item_id)) {
        const overdueQty = overdueMap.get(item.item_id)!;
        const dispenseQty = item.quantity - overdueQty;

        // INSERT ส่วนที่ค้างจ่าย
        await client.query(
          `INSERT INTO ${SCHEMA}.overdue_med
             (med_id, med_sid, patient_id, doctor_id, quantity, dispense_status, time)
           VALUES ($1, $2, $3, $4, $5, false, NOW())`,
          [item.med_id, item.med_sid, pr.patient_id, pr.doctor_id, overdueQty]
        );

        // ถ้ายังมีสต็อกพอจ่ายบางส่วน — ตัดสต็อกเฉพาะส่วนที่จ่ายได้
        if (dispenseQty > 0) {
          const { rows: drugRows } = await client.query(
            `SELECT med_sid, med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse
             WHERE med_sid = $1 FOR UPDATE`, [item.med_sid]
          );
          if (drugRows.length) {
            const balBefore = parseInt(drugRows[0].med_quantity);
            const consumed = await deductFefo(item.med_sid, dispenseQty, client);
            const balAfter = await recalcTotal(item.med_sid, client);
            const primaryLot = consumed[0];
            await client.query(
              `INSERT INTO ${SCHEMA}.stock_transactions
                 (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
                  lot_number, expiry_date, prescription_no, performed_by, note)
               VALUES ($1,$2,'out',$3,$4,$5,$6,$7,$8,$9,'จ่ายยาตามใบสั่ง (บางส่วน)')`,
              [item.med_sid, item.med_id, dispenseQty, balBefore, balAfter,
               primaryLot?.lot_number || null, primaryLot?.exp_date || null,
               pr.prescription_no, resolvedDispenser || null]
            );
          }
        }
        continue;
      }

      // ── ตัดสต็อกปกติ ──────────────────────────────────────────────────────
      const { rows: drugRows } = await client.query(
        `SELECT med_sid, med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse
         WHERE med_sid = $1 FOR UPDATE`, [item.med_sid]
      );
      if (!drugRows.length) throw new AppError(`ไม่พบยา med_sid: ${item.med_sid}`, 404);

      const drug = drugRows[0];
      const balBefore = parseInt(drug.med_quantity);
      if (item.quantity > balBefore)
        throw new AppError(
          `ยา ${item.med_sid} มีสต็อกไม่พอ (มี ${balBefore}, ต้องการ ${item.quantity})`, 400
        );

      // sync lot ถ้า med_stock_lots ไม่ครบ
      const { rows: lotRows } = await client.query(
        `SELECT COALESCE(SUM(quantity), 0)::int AS lot_total
         FROM ${SCHEMA}.med_stock_lots WHERE med_sid = $1`, [item.med_sid]
      );
      if (parseInt(lotRows[0].lot_total) < balBefore) {
        await client.query(
          `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, note)
           VALUES ($1, 'SYNC-' || to_char(NOW(), 'YYYYMMDD'), $2, 'ซิงค์สต็อก (auto)')`,
          [item.med_sid, balBefore - parseInt(lotRows[0].lot_total)]
        );
      }

      // FEFO deduction across lots
      const consumed = await deductFefo(item.med_sid, item.quantity, client);

      const balAfter = await recalcTotal(item.med_sid, client);

      // Record transaction with lot info from first (earliest-expiry) lot consumed
      const primaryLot = consumed[0];
      await client.query(
        `INSERT INTO ${SCHEMA}.stock_transactions
           (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
            lot_number, expiry_date, prescription_no, performed_by, note)
         VALUES ($1,$2,'out',$3,$4,$5,$6,$7,$8,$9,'จ่ายยาตามใบสั่ง')`,
        [item.med_sid, item.med_id, item.quantity, balBefore, balAfter,
        primaryLot?.lot_number || null,
        primaryLot?.exp_date || null,
        pr.prescription_no, resolvedDispenser || null]
      );
    }

    // update prescription status
    const { rows: updated } = await client.query(
      `UPDATE ${SCHEMA}.prescriptions
       SET status = 'dispensed', dispensed_by = $1, dispensed_at = NOW(), updated_at = NOW()
       WHERE prescription_id = $2 RETURNING *`,
      [resolvedDispenser || null, id]
    );
    const rx = updated[0];

    await client.query('COMMIT');

    // หาหรือสร้างคิว แล้ว auto-call ทันที (จ่ายยา = ยาพร้อม = เรียกผู้ป่วยมารับ)
    // ทำนอก transaction หลัก — ถ้า queue ล้มเหลวไม่ส่งผลให้ dispense ย้อนกลับ
    let queue_number: string | null = null;
    if (rx.patient_id) {
      try {
        const { rows: existingQ } = await client.query(
          `SELECT queue_id, queue_number FROM ${SCHEMA}.queue_entries
           WHERE patient_id = $1 AND status IN ('waiting','called')
             AND DATE(created_at AT TIME ZONE 'Asia/Bangkok') = CURRENT_DATE AT TIME ZONE 'Asia/Bangkok'
           ORDER BY queue_id ASC LIMIT 1`,
          [rx.patient_id]
        );
        let queue_id: number;
        if (existingQ.length) {
          queue_id    = existingQ[0].queue_id;
          queue_number = existingQ[0].queue_number;
        } else {
          queue_number = await nextQueueNumber(client, rx.ward);
          const { rows: newQ } = await client.query(
            `INSERT INTO ${SCHEMA}.queue_entries (queue_number, patient_id, note)
             VALUES ($1, $2, $3) RETURNING queue_id`,
            [queue_number, rx.patient_id, `ใบสั่งยา ${rx.prescription_no}`]
          );
          queue_id = newQ[0].queue_id;
        }
        await client.query(
          `UPDATE ${SCHEMA}.queue_entries
           SET status = 'completed', called_at = COALESCE(called_at, NOW()), called_by = COALESCE(called_by, $2),
               completed_at = NOW()
           WHERE queue_id = $1 AND status != 'completed'`,
          [queue_id, resolvedDispenser || null]
        );
        // บันทึก queue_number ลง prescriptions เพื่อใช้แสดงย้อนหลัง
        if (queue_number) {
          await client.query(
            `UPDATE ${SCHEMA}.prescriptions SET queue_number = $1 WHERE prescription_id = $2`,
            [queue_number, rx.prescription_id]
          );
        }
      } catch (qErr) {
        console.error('[dispense] queue auto-call failed (dispense still succeeded):', qErr);
      }
    }

    res.json({ ...rx, queue_number });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) { }
    next(err);
  } finally { client.release(); }
}

// ── POST /dispense/:id/return — คืนยา ────────────────────────────────────────
export async function returnPrescription(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { id } = req.params;
    const { performed_by, note, items } = req.body;
    // ถ้าไม่มี performed_by ใน body ให้ใช้ uid จาก JWT/session token แทน
    const resolvedPerformer = performed_by != null
      ? await resolveUserId(performed_by)
      : ((req as any).currentUser?.id ?? null);
    // items: [{ med_sid, quantity }] ส่วนที่คืน

    const { rows: prRows } = await client.query(
      `SELECT * FROM ${SCHEMA}.prescriptions WHERE prescription_id = $1 FOR UPDATE`, [id]
    );
    if (!prRows.length) throw new AppError('ไม่พบใบสั่งยา', 404);
    if (prRows[0].status !== 'dispensed')
      throw new AppError('สามารถคืนยาได้เฉพาะใบสั่งที่จ่ายแล้ว', 400);

    for (const item of (items || [])) {
      const { rows: drugRows } = await client.query(
        `SELECT med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`,
        [item.med_sid]
      );
      if (!drugRows.length) continue;
      const balBefore = parseInt(drugRows[0].med_quantity);

      // สร้าง lot คืนยา (ไม่มี exp_date — เรียงท้ายสุดใน FEFO)
      await client.query(
        `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, note)
         VALUES ($1, 'RET-' || to_char(NOW(), 'YYYYMMDD-HH24MISS'), $2, $3)`,
        [item.med_sid, parseInt(item.quantity), note || 'คืนยา']
      );

      const balAfter = await recalcTotal(item.med_sid, client);

      await client.query(
        `INSERT INTO ${SCHEMA}.stock_transactions
           (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
            prescription_no, performed_by, note)
         VALUES ($1,$2,'return',$3,$4,$5,$6,$7,$8)`,
        [item.med_sid, drugRows[0].med_id, item.quantity, balBefore, balAfter,
        prRows[0].prescription_no, resolvedPerformer || null, note]
      );
    }

    const { rows: updated } = await client.query(
      `UPDATE ${SCHEMA}.prescriptions SET status = 'returned', updated_at = NOW()
       WHERE prescription_id = $1 RETURNING *`, [id]
    );

    await client.query('COMMIT');
    res.json(updated[0]);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) { }
    next(err);
  } finally { client.release(); }
}

// ── POST /dispense/:id/cancel ─────────────────────────────────────────────────
export async function cancelPrescription(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { reason } = req.body;

    const { rows } = await query(
      `UPDATE ${SCHEMA}.prescriptions
       SET status = 'cancelled', note = COALESCE($1, note), updated_at = NOW()
       WHERE prescription_id = $2 AND status = 'pending'
       RETURNING *`,
      [reason, id]
    );
    if (!rows.length) throw new AppError('ไม่พบใบสั่งยาหรือสถานะไม่ใช่ pending', 400);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── GET /dispense/wards — รายชื่อวอร์ดจาก prescription ──────────────────────
export async function getWards(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(
      `SELECT DISTINCT ward FROM ${SCHEMA}.prescriptions
       WHERE ward IS NOT NULL ORDER BY ward`
    );
    res.json(rows.map(r => r.ward));
  } catch (err) { next(err); }
}

// ── GET /dispense/patients — ค้นหาผู้ป่วย ────────────────────────────────────
export async function searchPatients(req: Request, res: Response, next: NextFunction) {
  try {
    const { q } = req.query;
    if (!q) { res.json([]); return; }
    const { rows } = await query(
      `SELECT patient_id, hn_number, first_name, last_name,
              CONCAT(first_name,' ',last_name) AS full_name, national_id, phone
       FROM ${SCHEMA}.patient
       WHERE first_name ILIKE $1 OR last_name ILIKE $1
          OR hn_number ILIKE $1 OR national_id ILIKE $1
       LIMIT 20`,
      [`%${q}%`]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /dispense/:id/safety-check
// ตรวจ allergy + drug interaction สำหรับผู้ป่วยในใบสั่งยา
// ─────────────────────────────────────────────────────────────────────────────
export async function safetyCheck(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;

    // ดึงใบสั่งยา + ผู้ป่วย
    const { rows: rxRows } = await query(
      `SELECT pr.prescription_id, pr.patient_id, pr.prescription_no,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name,
              pa.hn_number, pa.blood_group, pa."PMH"
       FROM   ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = pr.patient_id
       WHERE  pr.prescription_id = $1`, [id]
    );
    if (!rxRows.length) throw new AppError('ไม่พบใบสั่งยา', 404);
    const rx = rxRows[0];

    // ดึงรายการยาในใบสั่ง
    const { rows: rxItems } = await query(
      `SELECT pi.med_sid, pi.med_id, pi.quantity, pi.dose, pi.frequency,
              mt.med_name, mt.med_generic_name, mt.med_medical_category,
              mt.med_severity, mt.med_pregnancy_category,
              ms.med_quantity AS stock_available,
              ms.exp_date, ms.is_expired
       FROM   ${SCHEMA}.prescription_items pi
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
       JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = pi.med_sid
       WHERE  pi.prescription_id = $1`, [id]
    );

    const alerts: any[] = [];
    const medIds = rxItems.map((i: any) => i.med_id);

    // ── ตรวจ allergy + ADR + interaction พร้อมกัน (parallel) ─────────────────
    const [allergyHits, adrHits, interactionHits] = await Promise.all([
      rx.patient_id && medIds.length
        ? query(
          `SELECT ar.allr_id, ar.med_id, ar.severity, ar.symptoms, ar.description,
                    mt.med_name, mt.med_generic_name
             FROM   ${SCHEMA}.allergy_registry ar
             JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ar.med_id
             WHERE  ar.patient_id = $1 AND ar.med_id = ANY($2::int[])`,
          [rx.patient_id, medIds]
        ).then(r => r.rows)
        : Promise.resolve([]),
      rx.patient_id && medIds.length
        ? query(
          `SELECT adr.description, adr.severity, adr.symptoms, mt.med_name
             FROM   ${SCHEMA}.adr_registry adr
             JOIN   ${SCHEMA}.med_table mt ON mt.med_id = adr.med_id
             WHERE  adr.patient_id = $1 AND adr.med_id = ANY($2::int[])`,
          [rx.patient_id, medIds]
        ).then(r => r.rows)
        : Promise.resolve([]),
      medIds.length > 1
        ? query(
          `WITH expanded AS (
             SELECT orig.med_id AS original_id, other.med_id AS expanded_id
             FROM   ${SCHEMA}.med_table orig
             JOIN   ${SCHEMA}.med_table other
                    ON other.med_generic_name = orig.med_generic_name
                   AND orig.med_generic_name IS NOT NULL
             WHERE  orig.med_id = ANY($1::int[])
           )
           SELECT DISTINCT mi.interaction_type, mi.severity, mi.description,
             mt1.med_name AS drug1, mt2.med_name AS drug2
           FROM   ${SCHEMA}.med_interaction mi
           JOIN   ${SCHEMA}.med_table mt1 ON mt1.med_id = mi.med_id_1
           JOIN   ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
           JOIN   expanded e1 ON e1.expanded_id = mi.med_id_1
           JOIN   expanded e2 ON e2.expanded_id = mi.med_id_2
           WHERE  mi.is_active = true
             AND  e1.original_id <> e2.original_id`,
          [medIds]
        ).then(r => r.rows)
        : Promise.resolve([]),
    ]);

    for (const hit of allergyHits) {
      alerts.push({
        type: 'allergy',
        level: hit.severity === 'severe' ? 'critical' : hit.severity === 'moderate' ? 'warning' : 'info',
        title: `⚠ แพ้ยา: ${hit.med_name}`, detail: hit.symptoms,
        note: hit.description, med_name: hit.med_name, severity: hit.severity,
      });
    }
    for (const hit of adrHits) {
      alerts.push({
        type: 'adr',
        level: hit.severity === 'severe' ? 'critical' : 'warning',
        title: `🔬 ADR: ${hit.med_name}`, detail: hit.symptoms || hit.description,
        note: hit.description, med_name: hit.med_name, severity: hit.severity,
      });
    }
    for (const hit of interactionHits) {
      alerts.push({
        type: 'interaction',
        level: hit.interaction_type === 'incompatible' ? 'critical' : hit.severity === 'severe' ? 'critical' : 'warning',
        title: `💊 ปฏิกิริยา: ${hit.drug1} + ${hit.drug2}`, detail: hit.description,
        note: `ประเภท: ${hit.interaction_type}`, interaction_type: hit.interaction_type, severity: hit.severity,
      });
    }

    // ── 4. ตรวจสต็อกไม่พอ ────────────────────────────────────────────────────
    for (const item of rxItems) {
      if (item.is_expired || (item.exp_date && new Date(item.exp_date) < new Date())) {
        alerts.push({
          type: 'expired',
          level: 'critical',
          title: `🚫 ยาหมดอายุ: ${item.med_name}`,
          detail: `ยาหมดอายุแล้ว กรุณาเลือกยา lot ใหม่`,
          med_name: item.med_name,
        });
      } else if (item.stock_available < item.quantity) {
        alerts.push({
          type: 'stock',
          level: 'critical',
          title: `📦 สต็อกไม่พอ: ${item.med_name}`,
          detail: `ต้องการ ${item.quantity} มีเพียง ${item.stock_available}`,
          med_name: item.med_name,
          required: item.quantity,
          available: item.stock_available,
        });
      }

      // ── 5. ยาหมวดพิเศษ ──────────────────────────────────────────────────────
      if (item.med_severity?.includes('เสพติด')) {
        alerts.push({
          type: 'narcotic',
          level: 'info',
          title: `📋 ยาเสพติด: ${item.med_name}`,
          detail: 'ต้องบันทึกในทะเบียนยาเสพติดและมีลายเซ็นผู้รับ',
          med_name: item.med_name,
        });
      }
      if (item.med_pregnancy_category === 'X') {
        alerts.push({
          type: 'pregnancy',
          level: 'critical',
          title: `🤰 ห้ามใช้ในหญิงตั้งครรภ์: ${item.med_name}`,
          detail: 'Pregnancy Category X — ห้ามใช้ในหญิงตั้งครรภ์หรือกำลังจะตั้งครรภ์',
          med_name: item.med_name,
        });
      } else if (['D'].includes(item.med_pregnancy_category)) {
        alerts.push({
          type: 'pregnancy',
          level: 'warning',
          title: `🤰 ระวังในหญิงตั้งครรภ์: ${item.med_name}`,
          detail: `Pregnancy Category ${item.med_pregnancy_category} — มีความเสี่ยง ต้องชั่งน้ำหนักผลดีผลเสีย`,
          med_name: item.med_name,
        });
      }
    }

    // สรุประดับความรุนแรงรวม
    const hasCritical = alerts.some(a => a.level === 'critical');
    const hasWarning = alerts.some(a => a.level === 'warning');

    res.json({
      prescription_id: rx.prescription_id,
      prescription_no: rx.prescription_no,
      patient_name: rx.patient_name,
      hn_number: rx.hn_number,
      blood_group: rx.blood_group,
      pmh: rx.PMH,
      alert_level: hasCritical ? 'critical' : hasWarning ? 'warning' : 'safe',
      alert_count: alerts.length,
      alerts,
      items: rxItems,
    });
  } catch (err) { next(err); }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /dispense/mock — สุ่มใบสั่งยาจำลอง (สำหรับทดสอบ)
// สุ่มผู้ป่วย + ยา + สร้างใบสั่งยา pending
// ─────────────────────────────────────────────────────────────────────────────
export async function createMockPrescription(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { count = 1 } = req.body; // จำนวนใบสั่งที่ต้องการสุ่ม (max 5)
    const n = Math.min(5, Math.max(1, parseInt(String(count))));

    // สุ่มผู้ป่วย (ไม่ซ้ำกัน)
    const { rows: patients } = await client.query(
      `SELECT patient_id, hn_number, CONCAT(first_name,' ',last_name) AS full_name
       FROM   ${SCHEMA}.patient
       ORDER  BY RANDOM() LIMIT $1`, [n]
    );
    if (!patients.length) throw new AppError('ไม่พบข้อมูลผู้ป่วยในระบบ กรุณา seed ข้อมูลก่อน', 400);

    // สุ่มแพทย์
    const { rows: doctors } = await client.query(
      `SELECT p.id FROM public.profiles p WHERE p.role_id = 4 ORDER BY RANDOM() LIMIT 1`
    );
    const doctorId = doctors[0]?.id || null;

    // สุ่มยาจากคลัง (มีสต็อก ไม่หมดอายุ)
    const { rows: drugs } = await client.query(
      `SELECT ms.med_sid, ms.med_id, ms.med_quantity, ms.med_showname,
              mt.med_name, mt.med_counting_unit, mt.med_dosage_form
       FROM   ${SCHEMA}.med_subwarehouse ms
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       WHERE  ms.med_quantity > 0
         AND  ms.is_expired = false
         AND  (ms.exp_date IS NULL OR ms.exp_date > CURRENT_DATE)
       ORDER  BY RANDOM() LIMIT 20`
    );
    if (!drugs.length) throw new AppError('ไม่พบยาในคลัง กรุณา seed ข้อมูลก่อน', 400);

    // Auto-reset per-prefix counters เมื่อยังไม่มีคิววันนี้ (เหมือน createQueue)
    await client.query(
      `UPDATE ${SCHEMA}.system_settings
       SET value = '0', updated_at = NOW()
       WHERE key LIKE 'queue_current_number_%'
         AND NOT EXISTS (
           SELECT 1 FROM ${SCHEMA}.queue_entries
           WHERE DATE(created_at AT TIME ZONE 'Asia/Bangkok')
               = (NOW() AT TIME ZONE 'Asia/Bangkok')::date
         )`
    );

    const wards = ['OPD', 'IPD', 'ER', 'ICU', 'หอผู้ป่วยชาย', 'หอผู้ป่วยหญิง', 'กุมารเวช', 'สูตินรีเวช'];
    const created = [];

    for (const patient of patients) {
      // สุ่มจำนวนยา 1–4 รายการต่อใบ
      const itemCount = Math.floor(Math.random() * 4) + 1;
      const shuffled = [...drugs].sort(() => Math.random() - 0.5);
      const picked = shuffled.slice(0, Math.min(itemCount, shuffled.length));

      // สร้าง prescription_no — ใช้ timestamp + random suffix กัน duplicate
      const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      const timeStr = Date.now().toString(36).toUpperCase().slice(-4);
      const randStr = Math.random().toString(36).slice(2, 4).toUpperCase();
      const rx_no: string = `RX-${dateStr}-${timeStr}${randStr}`;

      // INSERT prescription
      const { rows: rxRows } = await client.query<Record<string, any>>(
        `INSERT INTO ${SCHEMA}.prescriptions
           (prescription_no, patient_id, doctor_id, ward, status, note)
         VALUES ($1,$2,$3,$4,'pending',$5) RETURNING *`,
        [rx_no, patient.patient_id, doctorId,
          wards[Math.floor(Math.random() * wards.length)],
          'ใบสั่งยาทดสอบ — สร้างโดยระบบ Mock']
      );
      const rx: Record<string, any> = rxRows[0];

      // INSERT items
      const frequencies = ['OD', 'BID', 'TID', 'QID', 'PRN', 'HS'];
      const routes = ['รับประทาน', 'ฉีดเข้ากล้าม', 'ฉีดเข้าเส้นเลือด', 'พ่น'];

      for (const drug of picked) {
        const maxQty = Math.min(30, Math.floor(drug.med_quantity * 0.3) || 1);
        const qty = Math.max(1, Math.floor(Math.random() * maxQty) + 1);
        await client.query(
          `INSERT INTO ${SCHEMA}.prescription_items
             (prescription_id, med_sid, med_id, quantity, dose, frequency, route)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
          [rx.prescription_id, drug.med_sid, drug.med_id, qty,
          `${qty} ${drug.med_counting_unit || 'เม็ด'}`,
          frequencies[Math.floor(Math.random() * frequencies.length)],
          routes[Math.floor(Math.random() * routes.length)]]
        );
      }

      // สร้าง queue entry — ใช้คิวเดิมถ้ามีอยู่แล้ว (waiting/called วันนี้)
      const { rows: existingQ } = await client.query(
        `SELECT queue_id, queue_number FROM ${SCHEMA}.queue_entries
         WHERE patient_id = $1 AND status IN ('waiting','called')
           AND DATE(created_at AT TIME ZONE 'Asia/Bangkok') = (NOW() AT TIME ZONE 'Asia/Bangkok')::date
         ORDER BY queue_id ASC LIMIT 1`,
        [patient.patient_id]
      );

      let queueNumber: string;
      let queue_id: number;
      if (existingQ.length) {
        queueNumber = existingQ[0].queue_number;
        queue_id    = existingQ[0].queue_id;
      } else {
        queueNumber = await nextQueueNumber(client, rx.ward);
        const { rows: qRows } = await client.query(
          `INSERT INTO ${SCHEMA}.queue_entries (queue_number, patient_id, note)
           VALUES ($1, $2, $3) RETURNING queue_id`,
          [queueNumber, patient.patient_id, `ใบสั่งยา ${rx_no}`]
        );
        queue_id = qRows[0].queue_id;
      }

      // บันทึก queue_number ลง prescriptions เพื่อแสดงย้อนหลัง
      await client.query(
        `UPDATE ${SCHEMA}.prescriptions SET queue_number = $1 WHERE prescription_id = $2`,
        [queueNumber, rx.prescription_id]
      );

      created.push({
        prescription_id: rx.prescription_id,
        prescription_no: rx_no,
        patient_name: patient.full_name,
        hn_number: patient.hn_number,
        ward: rx.ward,
        item_count: picked.length,
        drugs: picked.map((d: any) => d.med_showname || d.med_name),
        queue_number: queueNumber,
        queue_id,
      });
    }

    await client.query('COMMIT');
    res.status(201).json({
      message: `สร้างใบสั่งยาจำลอง ${created.length} ใบเรียบร้อย`,
      created,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) { }
    next(err);
  } finally { client.release(); }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /dispense/safety-check-inline
// ตรวจ safety แบบ real-time ขณะสร้างใบสั่งยา (ก่อน submit)
// body: { patient_id, med_sids: number[] }
// ─────────────────────────────────────────────────────────────────────────────
export async function safetyCheckInline(req: Request, res: Response, next: NextFunction) {
  try {
    const { patient_id, med_sids = [] } = req.body;
    if (!med_sids.length) { res.json({ alert_level: 'safe', alert_count: 0, alerts: [], items: [] }); return; }

    // resolve patient
    const resolvedPatient = patient_id ? await resolvePatientId(patient_id) : null;

    // ดึงข้อมูลยาจาก med_sid
    const { rows: drugRows } = await query(
      `SELECT ms.med_sid, ms.med_id, ms.med_quantity AS stock_available,
              ms.exp_date, ms.is_expired,
              mt.med_name, mt.med_generic_name, mt.med_medical_category,
              mt.med_severity, mt.med_pregnancy_category, mt.med_counting_unit AS unit
       FROM   ${SCHEMA}.med_subwarehouse ms
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       WHERE  ms.med_sid = ANY($1::int[])`,
      [med_sids]
    );

    const alerts: any[] = [];
    const medIds = drugRows.map((d: any) => d.med_id);

    // ── 1. ตรวจแพ้ยา ─────────────────────────────────────────────────────────
    if (resolvedPatient && medIds.length > 0) {
      const { rows: allergyHits } = await query(
        `SELECT ar.severity, ar.symptoms, ar.description, mt.med_name
         FROM   ${SCHEMA}.allergy_registry ar
         JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ar.med_id
         WHERE  ar.patient_id = $1 AND ar.med_id = ANY($2::int[])`,
        [resolvedPatient, medIds]
      );
      for (const h of allergyHits) {
        alerts.push({
          type: 'allergy',
          level: h.severity === 'severe' ? 'critical' : h.severity === 'moderate' ? 'warning' : 'info',
          title: `⚠ แพ้ยา: ${h.med_name}`,
          detail: h.symptoms,
          note: h.description,
          med_name: h.med_name,
        });
      }

      // ── 2. ตรวจ ADR ──────────────────────────────────────────────────────────
      const { rows: adrHits } = await query(
        `SELECT adr.severity, adr.symptoms, adr.description, mt.med_name
         FROM   ${SCHEMA}.adr_registry adr
         JOIN   ${SCHEMA}.med_table mt ON mt.med_id = adr.med_id
         WHERE  adr.patient_id = $1 AND adr.med_id = ANY($2::int[])`,
        [resolvedPatient, medIds]
      );
      for (const h of adrHits) {
        alerts.push({
          type: 'adr',
          level: h.severity === 'severe' ? 'critical' : 'warning',
          title: `🔬 ADR: ${h.med_name}`,
          detail: h.symptoms || h.description,
          note: h.description,
          med_name: h.med_name,
        });
      }
    }

    // ── 3. ตรวจปฏิกิริยา (ขยายผ่าน generic_name เพื่อครอบคลุมทุกขนาด) ──────────
    if (medIds.length > 1) {
      const { rows: intHits } = await query(
        `WITH expanded AS (
           SELECT orig.med_id AS original_id, other.med_id AS expanded_id
           FROM   ${SCHEMA}.med_table orig
           JOIN   ${SCHEMA}.med_table other
                  ON other.med_generic_name = orig.med_generic_name
                 AND orig.med_generic_name IS NOT NULL
           WHERE  orig.med_id = ANY($1::int[])
         )
         SELECT DISTINCT mi.interaction_type, mi.severity, mi.description,
           mt1.med_name AS d1, mt2.med_name AS d2
         FROM   ${SCHEMA}.med_interaction mi
         JOIN   ${SCHEMA}.med_table mt1 ON mt1.med_id = mi.med_id_1
         JOIN   ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
         JOIN   expanded e1 ON e1.expanded_id = mi.med_id_1
         JOIN   expanded e2 ON e2.expanded_id = mi.med_id_2
         WHERE  mi.is_active = true
           AND  e1.original_id <> e2.original_id`,
        [medIds]
      );
      for (const h of intHits) {
        alerts.push({
          type: 'interaction',
          level: h.interaction_type === 'incompatible' ? 'critical' : 'warning',
          title: `💊 ปฏิกิริยา: ${h.d1} + ${h.d2}`,
          detail: h.description,
          note: `ประเภท: ${h.interaction_type}`,
        });
      }
    }

    // ── 4. ตรวจสต็อก + อายุยา + ยาพิเศษ ─────────────────────────────────────
    for (const d of drugRows) {
      if (d.is_expired || (d.exp_date && new Date(d.exp_date) < new Date()))
        alerts.push({ type: 'expired', level: 'critical', title: `🚫 หมดอายุ: ${d.med_name}`, detail: 'ยาหมดอายุ กรุณาใช้ lot ใหม่', med_name: d.med_name });
      if (d.med_severity?.includes('เสพติด'))
        alerts.push({ type: 'narcotic', level: 'info', title: `📋 ยาเสพติด: ${d.med_name}`, detail: 'ต้องบันทึกในทะเบียนยาเสพติด', med_name: d.med_name });
      if (d.med_pregnancy_category === 'X')
        alerts.push({ type: 'pregnancy', level: 'critical', title: `🤰 ห้ามในหญิงตั้งครรภ์: ${d.med_name}`, detail: 'Pregnancy Category X', med_name: d.med_name });
      else if (d.med_pregnancy_category === 'D')
        alerts.push({ type: 'pregnancy', level: 'warning', title: `🤰 ระวังในหญิงตั้งครรภ์: ${d.med_name}`, detail: 'Pregnancy Category D', med_name: d.med_name });
    }

    const hasCritical = alerts.some(a => a.level === 'critical');
    const hasWarning = alerts.some(a => a.level === 'warning');
    res.json({
      alert_level: hasCritical ? 'critical' : hasWarning ? 'warning' : 'safe',
      alert_count: alerts.length,
      alerts,
      items: drugRows,
    });
  } catch (err) { next(err); }
}

// ─────────────────────────────────────────────────────────────────────────────
// PUT /dispense/:id/items — แก้ไขรายการยาในใบสั่ง (pending เท่านั้น)
// body: { items: [{ item_id?, med_sid, quantity, dose, frequency, route }] }
// ─────────────────────────────────────────────────────────────────────────────
export async function updatePrescriptionItems(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { id } = req.params;
    const { items } = req.body;
    if (!Array.isArray(items) || !items.length) throw new AppError('items จำเป็น', 400);

    // ตรวจสถานะ — แก้ได้เฉพาะ pending
    const { rows: rxRows } = await client.query(
      `SELECT prescription_id, status, patient_id FROM ${SCHEMA}.prescriptions WHERE prescription_id = $1`, [id]
    );
    if (!rxRows.length) throw new AppError('ไม่พบใบสั่งยา', 404);
    if (rxRows[0].status !== 'pending') throw new AppError('แก้ไขได้เฉพาะใบสั่งที่ pending', 400);

    // ลบของเก่าออกทั้งหมด แล้ว insert ใหม่
    await client.query(`DELETE FROM ${SCHEMA}.prescription_items WHERE prescription_id = $1`, [id]);

    for (const item of items) {
      if (!item.med_sid || !item.quantity || item.quantity <= 0)
        throw new AppError('แต่ละรายการต้องมี med_sid และ quantity > 0', 400);

      const { rows: drugRows } = await client.query(
        `SELECT med_id, med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`, [item.med_sid]
      );
      if (!drugRows.length) throw new AppError(`ไม่พบยา med_sid: ${item.med_sid}`, 404);

      await client.query(
        `INSERT INTO ${SCHEMA}.prescription_items
           (prescription_id, med_sid, med_id, quantity, dose, frequency, route)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [id, item.med_sid, drugRows[0].med_id, item.quantity,
          item.dose || '', item.frequency || 'OD', item.route || 'รับประทาน']
      );
    }

    // อัปเดต updated_at
    await client.query(
      `UPDATE ${SCHEMA}.prescriptions SET updated_at = NOW() WHERE prescription_id = $1`, [id]
    );

    // return updated prescription + items
    const { rows: newItems } = await client.query(
      `SELECT pi.*,
              COALESCE(ms.unit_price, mt.med_selling_price, 0) AS unit_price,
              (pi.quantity::numeric * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS line_total,
              ms.med_showname, mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = pi.med_sid
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
       WHERE pi.prescription_id = $1`, [id]
    );

    await client.query('COMMIT');
    res.json({ prescription_id: parseInt(id), item_count: newItems.length, items: newItems });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) { }
    next(err);
  } finally { client.release(); }
}

// ─────────────────────────────────────────────────────────────────────────────
// PUT /dispense/:id/meta — แก้ไข metadata ใบสั่งยา (ward, note, diagnosis)
// ─────────────────────────────────────────────────────────────────────────────
export async function updatePrescriptionMeta(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { ward, note, diagnosis } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.prescriptions
       SET ward       = COALESCE($1, ward),
           note       = COALESCE($2, note),
           diagnosis  = COALESCE($3, diagnosis),
           updated_at = NOW()
       WHERE prescription_id = $4 AND status = 'pending'
       RETURNING *`,
      [ward, note, diagnosis, id]
    );
    if (!rows.length) throw new AppError('ไม่พบใบสั่งยา หรือสถานะไม่ใช่ pending', 400);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /dispense/:id/full — ดึงใบสั่งยาพร้อม items + safety check รวมกัน
// ─────────────────────────────────────────────────────────────────────────────
export async function getPrescriptionFull(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;

    // prescription + patient + doctor
    const { rows } = await query(
      `SELECT pr.*,
              pa.first_name, pa.last_name, pa.hn_number, pa.blood_group, pa."PMH",
              pa.photo AS patient_photo, pa.treatment_right, pa.treatment_right_note,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name,
              COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
              pdoc.firstname_th AS doctor_first,
              COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name
       FROM   ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient  pa    ON pa.patient_id = pr.patient_id
       LEFT JOIN public.profiles    pdoc  ON pdoc.id       = pr.doctor_id
       LEFT JOIN auth.users         adoc  ON adoc.id       = pr.doctor_id
       LEFT JOIN public.profiles    pdisp ON pdisp.id      = pr.dispensed_by
       LEFT JOIN auth.users         adisp ON adisp.id      = pr.dispensed_by
       WHERE  pr.prescription_id = $1`, [id]
    );
    if (!rows.length) throw new AppError('ไม่พบใบสั่งยา', 404);
    const rx = rows[0];

    // items
    const { rows: items } = await query(
      `SELECT pi.*,
              COALESCE(ms.unit_price, mt.med_selling_price, 0) AS unit_price,
              COALESCE(ms.unit_price, mt.med_selling_price, 0) AS current_unit_price,
              (pi.quantity::numeric * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS line_total,
              ms.med_showname, ms.med_showname_eng, ms.packaging_type,
              ms.med_quantity AS stock_available, ms.exp_date, ms.is_expired,
              mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit,
              mt.med_severity, mt.med_pregnancy_category, mt.med_medical_category
       FROM   ${SCHEMA}.prescription_items pi
       JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = pi.med_sid
       JOIN   ${SCHEMA}.med_table        mt ON mt.med_id  = pi.med_id
       WHERE  pi.prescription_id = $1`, [id]
    );

    // คำนวณ total_cost จาก items (unit_price × quantity)
    const total_cost = items.reduce(
      (sum: number, it: any) => sum + (Number(it.unit_price) || 0) * (Number(it.quantity) || 0),
      0
    );

    res.json({ ...rx, items, total_cost });
  } catch (err) { next(err); }
}

// ─────────────────────────────────────────────────────────────────────────────
// PUT /dispense/:id/meta
// ตรวจ allergy + interaction แบบ real-time ระหว่างสร้าง/แก้ใบสั่งยา
// body: { patient_id, med_ids: number[], med_sids: number[] }
// returns: { alerts_by_med_sid: { [med_sid]: Alert[] } }
// ─────────────────────────────────────────────────────────────────────────────
export async function liveSafetyCheck(req: Request, res: Response, next: NextFunction) {
  try {
    const { patient_id, med_ids = [], med_sids = [] } = req.body;
    const resolvedPatient = patient_id ? await resolvePatientId(patient_id) : null;

    // alerts_by_med_sid: { [med_sid]: Alert[] }
    const alertMap: Record<number, any[]> = {};
    med_sids.forEach((sid: number) => { alertMap[sid] = []; });

    if (!med_ids.length) {
      res.json({ alerts_by_med_sid: alertMap }); return;
    }

    // ── ตรวจ allergy + ADR + interaction + stock พร้อมกัน (parallel) ──────────
    const [allergyHits, adrHits, ixHits, stocks] = await Promise.all([
      resolvedPatient && med_ids.length
        ? query(
          `SELECT ar.med_id, ar.severity, ar.symptoms, mt.med_name
             FROM   ${SCHEMA}.allergy_registry ar
             JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ar.med_id
             WHERE  ar.patient_id = $1 AND ar.med_id = ANY($2::int[])`,
          [resolvedPatient, med_ids]
        ).then(r => r.rows)
        : Promise.resolve([]),
      resolvedPatient && med_ids.length
        ? query(
          `SELECT adr.med_id, adr.severity, adr.symptoms, adr.description, mt.med_name
             FROM   ${SCHEMA}.adr_registry adr
             JOIN   ${SCHEMA}.med_table mt ON mt.med_id = adr.med_id
             WHERE  adr.patient_id = $1 AND adr.med_id = ANY($2::int[])`,
          [resolvedPatient, med_ids]
        ).then(r => r.rows)
        : Promise.resolve([]),
      med_ids.length > 1
        ? query(
          `WITH expanded AS (
             -- ขยาย med_id ด้วย generic_name เพื่อครอบคลุมยาชื่อเดียวกันหลายขนาด
             SELECT orig.med_id AS original_id, other.med_id AS expanded_id
             FROM   ${SCHEMA}.med_table orig
             JOIN   ${SCHEMA}.med_table other
                    ON other.med_generic_name = orig.med_generic_name
                   AND orig.med_generic_name IS NOT NULL
             WHERE  orig.med_id = ANY($1::int[])
           )
           SELECT DISTINCT
             mi.med_id_1, mi.med_id_2, mi.interaction_type, mi.severity, mi.description,
             mt1.med_name AS drug1, mt2.med_name AS drug2,
             e1.original_id AS orig_id_1,
             e2.original_id AS orig_id_2
           FROM   ${SCHEMA}.med_interaction mi
           JOIN   ${SCHEMA}.med_table mt1 ON mt1.med_id = mi.med_id_1
           JOIN   ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
           JOIN   expanded e1 ON e1.expanded_id = mi.med_id_1
           JOIN   expanded e2 ON e2.expanded_id = mi.med_id_2
           WHERE  mi.is_active = true
             AND  e1.original_id <> e2.original_id`,
          [med_ids]
        ).then(r => r.rows)
        : Promise.resolve([]),
      med_sids.length
        ? query(
          `SELECT ms.med_sid, ms.med_quantity, ms.is_expired, ms.exp_date, mt.med_name
             FROM   ${SCHEMA}.med_subwarehouse ms
             JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
             WHERE  ms.med_sid = ANY($1::int[])`,
          [med_sids]
        ).then(r => r.rows)
        : Promise.resolve([]),
    ]);

    for (const hit of allergyHits) {
      const sids = med_sids.filter((_: number, i: number) => med_ids[i] === hit.med_id);
      for (const sid of sids) {
        alertMap[sid] = alertMap[sid] ?? [];
        alertMap[sid].push({
          type: 'allergy',
          level: hit.severity === 'severe' ? 'critical' : hit.severity === 'moderate' ? 'warning' : 'info',
          title: `⚠ แพ้ยา: ${hit.med_name}`, detail: hit.symptoms, severity: hit.severity,
        });
      }
    }
    for (const hit of adrHits) {
      const sids = med_sids.filter((_: number, i: number) => med_ids[i] === hit.med_id);
      for (const sid of sids) {
        alertMap[sid] = alertMap[sid] ?? [];
        alertMap[sid].push({
          type: 'adr',
          level: hit.severity === 'severe' ? 'critical' : 'warning',
          title: `🔬 ADR: ${hit.med_name}`, detail: hit.symptoms || hit.description, severity: hit.severity,
        });
      }
    }
    for (const hit of ixHits) {
      const level = hit.interaction_type === 'incompatible' ? 'critical' : hit.severity === 'severe' ? 'critical' : 'warning';
      const alert = {
        type: 'interaction', level,
        title: `💊 ${hit.drug1} + ${hit.drug2}`, detail: hit.description,
        severity: hit.severity, interaction_type: hit.interaction_type,
      };
      // ใช้ orig_id (med_id จริงในใบสั่ง) แทน med_id_1/2 ที่อาจเป็น variant อื่น
      for (const [mIdx, mId] of med_ids.entries()) {
        if (mId === hit.orig_id_1 || mId === hit.orig_id_2) {
          const sid = med_sids[mIdx];
          if (sid !== undefined) { alertMap[sid] = alertMap[sid] ?? []; alertMap[sid].push(alert); }
        }
      }
    }
    for (const s of stocks) {
      const isExpired = s.is_expired || (s.exp_date && new Date(s.exp_date) < new Date());
      if (isExpired) {
        alertMap[s.med_sid] = alertMap[s.med_sid] ?? [];
        alertMap[s.med_sid].push({ type: 'expired', level: 'critical', title: `🚫 ยาหมดอายุ: ${s.med_name}`, detail: 'กรุณาเลือก lot ใหม่' });
      } else if (s.med_quantity <= 5) {
        alertMap[s.med_sid] = alertMap[s.med_sid] ?? [];
        alertMap[s.med_sid].push({ type: 'stock', level: s.med_quantity === 0 ? 'critical' : 'warning', title: `📦 สต็อกต่ำ: ${s.med_name}`, detail: `คงเหลือ ${s.med_quantity} หน่วย` });
      }
    }

    res.json({ alerts_by_med_sid: alertMap });
  } catch (err) { next(err); }
}