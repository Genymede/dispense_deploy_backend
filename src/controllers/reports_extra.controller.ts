import { Request, Response, NextFunction } from 'express';
import { query, SCHEMA, paginate } from '../db/pool';
import { AppError } from '../middleware/errorHandler';
import { runCutOffLogic } from '../scheduler';

function df(v: unknown) { return typeof v === 'string' && v ? v : undefined; }

// ── รายงานทะเบียนยา (med_table) ──────────────────────────────────────────────
export async function reportMedTable(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, category } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (search) { w += ` AND (med_name ILIKE $${p} OR med_generic_name ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (category) { w += ` AND med_medical_category = $${p}`; params.push(category); p++; }
    const { rows } = await query(`SELECT *, (med_exp < CURRENT_DATE) AS is_expired FROM ${SCHEMA}.med_table ${w} ORDER BY med_name LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_table ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── รายงานคลังยาย่อย (med_subwarehouse) ──────────────────────────────────────
export async function reportMedSubwarehouse(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status, sub_warehouse_id } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (search) { w += ` AND (COALESCE(ms.med_showname, mt.med_name) ILIKE $${p} OR mt.med_generic_name ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (status === 'low') { w += ` AND ms.min_quantity IS NOT NULL AND ms.med_quantity < ms.min_quantity AND ms.is_expired=false`; }
    else if (status === 'expired') { w += ` AND (ms.is_expired=true OR ms.exp_date < NOW())`; }
    else if (status === 'out') { w += ` AND ms.med_quantity=0`; }
    if (sub_warehouse_id) { w += ` AND ms.sub_warehouse_id=$${p}`; params.push(sub_warehouse_id); p++; }
    const { rows } = await query(
      `SELECT ms.*, COALESCE(ms.med_showname, mt.med_name) AS drug_name,
              mt.med_generic_name, mt.med_medical_category AS category, mt.med_counting_unit AS unit,
              mt.med_selling_price AS list_price,
              (ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_value,
              CASE WHEN ms.is_expired OR ms.exp_date < NOW() THEN 'หมดอายุ'
                   WHEN ms.med_quantity=0 THEN 'หมดสต็อก'
                   WHEN ms.min_quantity IS NOT NULL AND ms.med_quantity < ms.min_quantity THEN 'ต่ำกว่าขั้นต่ำ'
                   ELSE 'ปกติ' END AS stock_status
       FROM ${SCHEMA}.med_subwarehouse ms
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=ms.med_id
       ${w} ORDER BY drug_name LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_subwarehouse ms JOIN ${SCHEMA}.med_table mt ON mt.med_id=ms.med_id ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── รายงานยาหมดอายุ ──────────────────────────────────────────────────────────
export async function reportMedExpired(req: Request, res: Response, next: NextFunction) {
  try {
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const { rows } = await query(
      `SELECT ms.*, COALESCE(ms.med_showname, mt.med_name) AS drug_name,
              mt.med_generic_name, mt.med_counting_unit AS unit,
              (ms.exp_date::date - CURRENT_DATE) AS days_since_expiry,
              (ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS lost_value
       FROM ${SCHEMA}.med_subwarehouse ms
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=ms.med_id
       WHERE ms.is_expired=true OR ms.exp_date < CURRENT_DATE
       ORDER BY ms.exp_date ASC LIMIT $1 OFFSET $2`, [limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_subwarehouse WHERE is_expired=true OR exp_date < CURRENT_DATE`);
    const summary = await query(`SELECT SUM(ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_lost_value, COUNT(*) AS count FROM ${SCHEMA}.med_subwarehouse ms JOIN ${SCHEMA}.med_table mt ON mt.med_id=ms.med_id WHERE ms.is_expired=true OR ms.exp_date < CURRENT_DATE`);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0'), summary: summary.rows[0] });
  } catch (err) { next(err); }
}



// ── ประวัติการจ่ายยา (1 แถว / รายการยา) ─────────────────────────────────────
export async function reportMedOrderHistory(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status, ward, date_from, date_to } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (search) {
      w += ` AND (pr.prescription_no ILIKE $${p}
               OR CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p}
               OR pa.hn_number ILIKE $${p}
               OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (df(status))    { w += ` AND pr.status=$${p}`;      params.push(status);    p++; }
    if (df(ward))      { w += ` AND pr.ward=$${p}`;        params.push(ward);      p++; }
    if (df(date_from)) { w += ` AND pr.dispensed_at>=$${p}`;        params.push(date_from); p++; }
    if (df(date_to))   { w += ` AND pr.dispensed_at<$${p}::date+1`; params.push(date_to);   p++; }

    const { rows } = await query(
      `SELECT
         pi.item_id,
         pr.prescription_no, pr.status, pr.ward, pr.diagnosis,
         pr.created_at, pr.dispensed_at,
         CONCAT(pa.first_name,' ',pa.last_name) AS patient_name,
         pa.hn_number,
         COALESCE(pdoc.firstname_th||' '||pdoc.lastname_th, adoc.email, '') AS doctor_name,
         COALESCE(pdisp.firstname_th||' '||pdisp.lastname_th, adisp.email, '') AS dispensed_by_name,
         mt.med_name,
         ms.med_showname,
         pi.quantity,
         pi.frequency,
         pi.route
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.prescriptions pr       ON pr.prescription_id = pi.prescription_id
       JOIN ${SCHEMA}.patient pa             ON pa.patient_id      = pr.patient_id
       JOIN ${SCHEMA}.med_subwarehouse ms    ON ms.med_sid         = pi.med_sid
       JOIN ${SCHEMA}.med_table mt           ON mt.med_id          = pi.med_id
       LEFT JOIN public.profiles pdoc        ON pdoc.id            = pr.doctor_id
       LEFT JOIN auth.users      adoc        ON adoc.id            = pr.doctor_id
       LEFT JOIN public.profiles pdisp       ON pdisp.id           = pr.dispensed_by
       LEFT JOIN auth.users      adisp       ON adisp.id           = pr.dispensed_by
       ${w}
       ORDER BY pr.dispensed_at DESC NULLS LAST, pr.prescription_id, pi.item_id
       LIMIT $${p} OFFSET $${p+1}`,
      [...params, limit, offset]
    );

    const cr = await query(
      `SELECT COUNT(*) AS total
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.prescriptions pr    ON pr.prescription_id = pi.prescription_id
       JOIN ${SCHEMA}.patient pa          ON pa.patient_id      = pr.patient_id
       JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid         = pi.med_sid
       JOIN ${SCHEMA}.med_table mt        ON mt.med_id          = pi.med_id
       ${w}`, params
    );
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── ประวัติการใช้ยาผู้ป่วย (prescription items level) ────────────────────────
export async function reportMedUsageHistory(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status, date_from, date_to, patient_id } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (patient_id) { w += ` AND pr.patient_id=$${p}`; params.push(Number(patient_id)); p++; }
    if (search) {
      w += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (status) { w += ` AND pr.status=$${p}`; params.push(status); p++; }
    if (df(date_from)) { w += ` AND pr.created_at>=$${p}`; params.push(date_from); p++; }
    if (df(date_to))   { w += ` AND pr.created_at<$${p}::date+1`; params.push(date_to); p++; }
    const { rows } = await query(
      `SELECT
         pi.item_id,
         pr.prescription_no, pr.status, pr.created_at,
         pr.ward, pr.diagnosis,
         CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
         COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name,
         mt.med_name, mt.med_generic_name,
         mt.med_counting_unit AS unit,
         pi.quantity, pi.dose, pi.frequency, pi.route
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.prescriptions pr ON pr.prescription_id = pi.prescription_id
       LEFT JOIN ${SCHEMA}.patient pa  ON pa.patient_id = pr.patient_id
       LEFT JOIN public.profiles pu    ON pu.id = pr.doctor_id
       LEFT JOIN auth.users     au     ON au.id = pr.doctor_id
       LEFT JOIN ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
       ${w}
       ORDER BY pr.created_at DESC, pa.last_name
       LIMIT $${p} OFFSET $${p+1}`,
      [...params, limit, offset]
    );
    const cr = await query(
      `SELECT COUNT(*) AS total
       FROM ${SCHEMA}.prescription_items pi
       JOIN ${SCHEMA}.prescriptions pr ON pr.prescription_id = pi.prescription_id
       LEFT JOIN ${SCHEMA}.patient pa  ON pa.patient_id = pr.patient_id
       LEFT JOIN ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
       ${w}`,
      params
    );
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── Medication Error ──────────────────────────────────────────────────────────
export async function reportMedError(req: Request, res: Response, next: NextFunction) {
  try {
    const { resolved, date_from, date_to } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (resolved !== undefined) { w += ` AND em.resolved=$${p}`; params.push(resolved === 'true'); p++; }
    if (df(date_from)) { w += ` AND em.time>=$${p}`; params.push(date_from); p++; }
    if (df(date_to)) { w += ` AND em.time<$${p}::date+1`; params.push(date_to); p++; }
    const { rows } = await query(
      `SELECT em.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name
       FROM ${SCHEMA}.error_medication em
       JOIN ${SCHEMA}.patient pa ON pa.patient_id=em.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=em.med_id
       LEFT JOIN public.profiles pu ON pu.id=em.doctor_id
       LEFT JOIN auth.users     au ON au.id=em.doctor_id
       ${w} ORDER BY em.time DESC LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.error_medication em JOIN ${SCHEMA}.patient pa ON pa.patient_id=em.patient_id JOIN ${SCHEMA}.med_table mt ON mt.med_id=em.med_id ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── Med Problem ───────────────────────────────────────────────────────────────
export async function reportMedProblem(req: Request, res: Response, next: NextFunction) {
  try {
    const { resolved, type } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (resolved !== undefined) { w += ` AND mp.is_resolved=$${p}`; params.push(resolved === 'true'); p++; }
    if (type) { w += ` AND mp.problem_type=$${p}`; params.push(type); p++; }
    const { rows } = await query(
      `SELECT mp.*, mt.med_name, mt.med_generic_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS reported_by_name
       FROM ${SCHEMA}.med_problem mp
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=mp.med_id
       LEFT JOIN public.profiles pu ON pu.id=mp.reported_by
       LEFT JOIN auth.users     au ON au.id=mp.reported_by
       ${w} ORDER BY mp.reported_at DESC LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_problem mp JOIN ${SCHEMA}.med_table mt ON mt.med_id=mp.med_id ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── Med Delivery ──────────────────────────────────────────────────────────────
export async function reportMedDelivery(req: Request, res: Response, next: NextFunction) {
  try {
    const { status, date_from, date_to } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (status) { w += ` AND md.status=$${p}`; params.push(status); p++; }
    if (df(date_from)) { w += ` AND md.delivery_date>=$${p}`; params.push(date_from); p++; }
    if (df(date_to)) { w += ` AND md.delivery_date<$${p}::date+1`; params.push(date_to); p++; }
    const { rows } = await query(
      `SELECT md.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number
       FROM ${SCHEMA}.med_delivery md
       JOIN ${SCHEMA}.patient pa ON pa.patient_id=md.patient_id
       ${w} ORDER BY md.delivery_date DESC LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_delivery md JOIN ${SCHEMA}.patient pa ON pa.patient_id=md.patient_id ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── Overdue Med ───────────────────────────────────────────────────────────────
export async function reportOverdueMed(req: Request, res: Response, next: NextFunction) {
  try {
    const { dispensed } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (dispensed !== undefined) { w += ` AND om.dispense_status=$${p}`; params.push(dispensed === 'true'); p++; }
    const { rows } = await query(
      `SELECT om.*, mt.med_name, mt.med_generic_name,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name,
              COALESCE(ms.med_showname, mt.med_name) AS sub_drug_name
       FROM ${SCHEMA}.overdue_med om
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=om.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id=om.patient_id
       LEFT JOIN public.profiles pu ON pu.id=om.doctor_id
       LEFT JOIN auth.users     au ON au.id=om.doctor_id
       LEFT JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid=om.med_sid
       ${w} ORDER BY om.time DESC LIMIT $${p} OFFSET $${p+1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total, SUM(CASE WHEN dispense_status=false THEN 1 ELSE 0 END) AS pending FROM ${SCHEMA}.overdue_med ${w}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0'), pending: parseInt(cr.rows[0]?.pending ?? '0') });
  } catch (err) { next(err); }
}

// ── Cut-off Period ────────────────────────────────────────────────────────────
export async function reportCutOff(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(
      `SELECT mcp.*, mcp.med_period_id AS cut_off_period_id, sw.name AS warehouse_name
       FROM ${SCHEMA}.med_cut_off_period mcp
       JOIN ${SCHEMA}.sub_warehouse sw ON sw.sub_warehouse_id=mcp.sub_warehouse_id
       ORDER BY mcp.period_month, mcp.period_day`);
    res.json({ data: rows, total: rows.length });
  } catch (err) { next(err); }
}

// ── POST /reports/cut-off/:id/execute ─────────────────────────────────────────
export async function executeCutOff(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;

    // ดึงข้อมูล period + warehouse
    const { rows: periods } = await query(
      `SELECT mcp.*, sw.name AS warehouse_name
       FROM ${SCHEMA}.med_cut_off_period mcp
       JOIN ${SCHEMA}.sub_warehouse sw ON sw.sub_warehouse_id = mcp.sub_warehouse_id
       WHERE mcp.med_period_id = $1`,
      [id]
    );
    if (!periods.length) throw new AppError('ไม่พบรายการ cut-off', 404);
    const period = periods[0];

    // รัน logic จริง
    const summary = await runCutOffLogic(period.sub_warehouse_id, period.warehouse_name);

    // อัปเดต timestamp หลัง logic สำเร็จ
    const { rows: updated } = await query(
      `UPDATE ${SCHEMA}.med_cut_off_period
       SET last_executed_at = NOW(), updated_at = NOW()
       WHERE med_period_id = $1
       RETURNING *, $2::text AS warehouse_name`,
      [id, period.warehouse_name]
    );

    res.json({
      message: 'ดำเนินการตัดรอบเรียบร้อย',
      data: updated[0],
      summary,
    });
  } catch (err) { next(err); }
}

// ── RAD Registry (rad_registry summary) ──────────────────────────────────────
export async function reportRadRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to, status } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = [];
    let w = 'WHERE 1=1'; let p = 1;

    if (status)        { w += ` AND rr.status = $${p}`;                  params.push(status);    p++; }
    if (df(date_from)) { w += ` AND rr.request_time >= $${p}`;           params.push(date_from); p++; }
    if (df(date_to))   { w += ` AND rr.request_time < $${p}::date + 1`; params.push(date_to);   p++; }

    const sumW = w.replace(/\brr\./g, '');

    const { rows } = await query(
      `SELECT rr.*,
              mt.med_name, mt.med_generic_name, mt.med_counting_unit,
              mt.med_medical_category AS category,
              pa.first_name || ' ' || pa.last_name AS patient_name, pa.hn_number,
              req.email AS requested_by_name,
              apr.email AS approved_by_name,
              stock_agg.current_stock
       FROM   ${SCHEMA}.rad_registry rr
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = rr.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = rr.patient_id
       LEFT JOIN auth.users req ON req.id = rr.requested_by
       LEFT JOIN auth.users apr ON apr.id = rr.approved_by
       LEFT JOIN (
         SELECT med_id, SUM(med_quantity) AS current_stock
         FROM ${SCHEMA}.med_subwarehouse
         WHERE is_expired = false
         GROUP BY med_id
       ) stock_agg ON stock_agg.med_id = rr.med_id
       ${w}
       ORDER BY rr.request_time DESC
       LIMIT $${p} OFFSET $${p + 1}`,
      [...params, limit, offset]
    );
    const cr = await query(
      `SELECT COUNT(*) AS total FROM ${SCHEMA}.rad_registry rr ${w}`,
      params
    );
    const summary = await query(
      `SELECT status, COUNT(*) AS count, SUM(quantity) AS total_qty
       FROM   ${SCHEMA}.rad_registry ${sumW}
       GROUP  BY status`,
      params
    );
    res.json({ data: rows, total: parseInt(cr.rows[0].total), summary: summary.rows });
  } catch (err) { next(err); }
}
