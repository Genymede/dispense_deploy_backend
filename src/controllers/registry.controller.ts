import { Request, Response, NextFunction } from 'express';
import { query, pool, SCHEMA, resolveUserId, resolvePatientId, resolveMedId, paginate } from '../db/pool';
import { AppError } from '../middleware/errorHandler';
import { deductFefo, recalcTotal } from './stock.controller';

// ═══ REGISTRY: ทะเบียนยาหลัก (med_table) ═══════════════════════════════════════

export async function getMedRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, category, severity, essential } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (med_name ILIKE $${p} OR med_generic_name ILIKE $${p} OR med_marketing_name ILIKE $${p} OR "med_TMT_code" ILIKE $${p} OR med_thai_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (category) { where += ` AND med_medical_category = $${p}`; params.push(category); p++; }
    if (severity) { where += ` AND med_severity = $${p}`; params.push(severity); p++; }
    if (essential === 'Y') { where += ` AND med_essential_med_list = 'Y'`; }
    const { rows } = await query(`SELECT * FROM ${SCHEMA}.med_table ${where} ORDER BY med_name LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_table ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

export async function getMedRegistryById(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id } = req.params;
    const [medR, subR, intR, allR] = await Promise.all([
      query(`SELECT * FROM ${SCHEMA}.med_table WHERE med_id = $1`, [med_id]),
      query(`SELECT * FROM ${SCHEMA}.med_subwarehouse WHERE med_id = $1 ORDER BY med_sid`, [med_id]),
      query(`SELECT mi.*, mt2.med_name AS interacts_with_name FROM ${SCHEMA}.med_interaction mi JOIN ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2 WHERE mi.med_id_1 = $1 AND mi.is_active = true`, [med_id]),
      query(`SELECT ar.*, CONCAT(p.first_name,' ',p.last_name) AS patient_name, p.hn_number FROM ${SCHEMA}.allergy_registry ar JOIN ${SCHEMA}.patient p ON p.patient_id = ar.patient_id WHERE ar.med_id = $1 ORDER BY ar.reported_at DESC LIMIT 20`, [med_id]),
    ]);
    if (!medR.rows.length) throw new AppError('ไม่พบทะเบียนยา', 404);
    res.json({ med: medR.rows[0], subwarehouse: subR.rows, interactions: intR.rows, allergies: allR.rows });
  } catch (err) { next(err); }
}

export async function getMedCategories(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(`SELECT DISTINCT med_medical_category AS category FROM ${SCHEMA}.med_table WHERE med_medical_category IS NOT NULL ORDER BY category`);
    res.json(rows.map(r => r.category));
  } catch (err) { next(err); }
}

// ═══ REGISTRY: การแพ้ยา ════════════════════════════════════════════════════════

export async function getAllergyRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, severity, patient_id } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p} OR ar.symptoms ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (severity) { where += ` AND ar.severity = $${p}`; params.push(severity); p++; }
    if (patient_id) { where += ` AND ar.patient_id = $${p}`; params.push(Number(patient_id)); p++; }
    const { rows } = await query(
      `SELECT ar.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number, pa.national_id,
              mt.med_name, mt.med_generic_name, mt.med_medical_category
       FROM ${SCHEMA}.allergy_registry ar
       JOIN ${SCHEMA}.patient pa ON pa.patient_id = ar.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = ar.med_id
       ${where} ORDER BY ar.created_at DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.allergy_registry ar JOIN ${SCHEMA}.patient pa ON pa.patient_id=ar.patient_id JOIN ${SCHEMA}.med_table mt ON mt.med_id=ar.med_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: ADR ════════════════════════════════════════════════════════════

export async function getAdrRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, severity, patient_id } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (severity)   { where += ` AND adr.severity = $${p}`;     params.push(severity);          p++; }
    if (patient_id) { where += ` AND adr.patient_id = $${p}`;   params.push(Number(patient_id)); p++; }
    const { rows } = await query(
      `SELECT adr.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name, mt.med_generic_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS reporter_name
       FROM ${SCHEMA}.adr_registry adr
       JOIN ${SCHEMA}.patient pa ON pa.patient_id = adr.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = adr.med_id
       LEFT JOIN public.profiles pu ON pu.id = adr.reporter_id
       LEFT JOIN auth.users     au ON au.id = adr.reporter_id
       ${where} ORDER BY adr.reported_at DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.adr_registry adr JOIN ${SCHEMA}.patient pa ON pa.patient_id=adr.patient_id JOIN ${SCHEMA}.med_table mt ON mt.med_id=adr.med_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: ปฏิกิริยายา ════════════════════════════════════════════════════

export async function getMedInteractions(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, type } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE mi.is_active = true'; let p = 1;
    if (type) { where += ` AND mi.interaction_type = $${p}`; params.push(type); p++; }
    if (search) {
      where += ` AND (mt1.med_name ILIKE $${p} OR mt2.med_name ILIKE $${p} OR mi.description ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    const { rows } = await query(
      `SELECT mi.*, mt1.med_name AS drug1_name, mt2.med_name AS drug2_name,
              mt1.med_generic_name AS drug1_generic, mt2.med_generic_name AS drug2_generic
       FROM ${SCHEMA}.med_interaction mi
       JOIN ${SCHEMA}.med_table mt1 ON mt1.med_id = mi.med_id_1
       JOIN ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
       ${where} ORDER BY mi.severity DESC, mt1.med_name LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_interaction mi JOIN ${SCHEMA}.med_table mt1 ON mt1.med_id=mi.med_id_1 JOIN ${SCHEMA}.med_table mt2 ON mt2.med_id=mi.med_id_2 ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: ประวัติใช้ยา ═══════════════════════════════════════════════════

export async function getMedUsage(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status, patient_id } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (patient_id) { where += ` AND mu.patient_id = $${p}`; params.push(Number(patient_id)); p++; }
    if (search) {
      where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (status) { where += ` AND mu.usage_status = $${p}`; params.push(status); p++; }
    const { rows } = await query(
      `SELECT mu.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name, mt.med_generic_name, mt.med_counting_unit AS unit
       FROM ${SCHEMA}.med_usage mu
       JOIN ${SCHEMA}.patient pa ON pa.patient_id = mu.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = mu.med_id
       ${where} ORDER BY mu.order_datetime DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_usage mu JOIN ${SCHEMA}.patient pa ON pa.patient_id=mu.patient_id JOIN ${SCHEMA}.med_table mt ON mt.med_id=mu.med_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: ประวัติจ่ายยา (prescriptions) ══════════════════════════════════

export async function getDispenseHistory(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status, ward, date_from, date_to } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) { where += ` AND (pr.prescription_no ILIKE $${p} OR CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (status) { where += ` AND pr.status = $${p}`; params.push(status); p++; }
    if (ward) { where += ` AND pr.ward = $${p}`; params.push(ward); p++; }
    if (date_from) { where += ` AND pr.dispensed_at >= $${p}`; params.push(date_from); p++; }
    if (date_to) { where += ` AND pr.dispensed_at < $${p}::date + 1`; params.push(date_to); p++; }
    const { rows } = await query(
      `SELECT pr.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
              COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name,
              COUNT(pi.item_id)::int AS item_count
       FROM ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient pa   ON pa.patient_id = pr.patient_id
       LEFT JOIN public.profiles pdoc   ON pdoc.id = pr.doctor_id
       LEFT JOIN auth.users     adoc   ON adoc.id = pr.doctor_id
       LEFT JOIN public.profiles pdisp  ON pdisp.id = pr.dispensed_by
       LEFT JOIN auth.users     adisp  ON adisp.id = pr.dispensed_by
       LEFT JOIN ${SCHEMA}.prescription_items pi ON pi.prescription_id = pr.prescription_id
       ${where} GROUP BY pr.prescription_id, pa.first_name, pa.last_name, pa.hn_number,
                         pdoc.firstname_th, pdoc.lastname_th, adoc.email,
                         pdisp.firstname_th, pdisp.lastname_th, adisp.email
       ORDER BY pr.dispensed_at DESC NULLS LAST LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.prescriptions pr LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id=pr.patient_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: การจัดส่งยา ════════════════════════════════════════════════════

export async function getMedDelivery(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) { where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR md.receiver_name ILIKE $${p} OR md.receiver_phone ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (status) { where += ` AND md.status = $${p}`; params.push(status); p++; }
    const { rows } = await query(
      `SELECT md.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number, pa.phone AS patient_phone, pa.photo AS patient_photo
       FROM ${SCHEMA}.med_delivery md
       JOIN ${SCHEMA}.patient pa ON pa.patient_id = md.patient_id
       ${where} ORDER BY md.delivery_date DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_delivery md JOIN ${SCHEMA}.patient pa ON pa.patient_id=md.patient_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: ยาค้างจ่าย (overdue) ══════════════════════════════════════════

export async function getOverdueMed(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, dispensed } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) { where += ` AND (mt.med_name ILIKE $${p} OR CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (dispensed !== undefined) { where += ` AND om.dispense_status = $${p}`; params.push(dispensed === 'true'); p++; }
    const { rows } = await query(
      `SELECT om.*, mt.med_name, mt.med_generic_name,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name,
              COALESCE(ms.med_showname, ms.packaging_type::text) AS sub_drug_name,
              ms.packaging_type, ms.location
       FROM ${SCHEMA}.overdue_med om
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = om.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = om.patient_id
       LEFT JOIN public.profiles pu ON pu.id = om.doctor_id
       LEFT JOIN auth.users     au ON au.id = om.doctor_id
       LEFT JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = om.med_sid
       ${where} ORDER BY om.time DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.overdue_med om JOIN ${SCHEMA}.med_table mt ON mt.med_id=om.med_id LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id=om.patient_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: RAD (rad_registry) ════════════════════════════════════════════

export async function getRadRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, status } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) { where += ` AND (mt.med_name ILIKE $${p} OR rr.diagnosis ILIKE $${p} OR req.email ILIKE $${p})`; params.push(`%${search}%`); p++; }
    if (status) { where += ` AND rr.status = $${p}`; params.push(status); p++; }
    const { rows } = await query(
      `SELECT rr.*,
              mt.med_name, mt.med_generic_name, mt.med_counting_unit,
              pa.first_name || ' ' || pa.last_name AS patient_name, pa.hn_number,
              req.email AS requested_by_name,
              apr.email AS approved_by_name
       FROM ${SCHEMA}.rad_registry rr
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = rr.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = rr.patient_id
       LEFT JOIN auth.users req ON req.id = rr.requested_by
       LEFT JOIN auth.users apr ON apr.id = rr.approved_by
       ${where} ORDER BY rr.request_time DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(
      `SELECT COUNT(*) AS total FROM ${SCHEMA}.rad_registry rr
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = rr.med_id
       LEFT JOIN auth.users req ON req.id = rr.requested_by
       ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══ REGISTRY: Medication Error ═══════════════════════════════════════════════

export async function getMedError(req: Request, res: Response, next: NextFunction) {
  try {
    const { resolved } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (resolved !== undefined && resolved !== '') {
      where += ` AND em.resolved = $${p}`; params.push(resolved === 'true'); p++;
    }
    const { rows } = await query(
      `SELECT em.*, CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name, mt.med_generic_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name
       FROM ${SCHEMA}.error_medication em
       JOIN ${SCHEMA}.patient pa ON pa.patient_id = em.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = em.med_id
       LEFT JOIN public.profiles pu ON pu.id = em.doctor_id
       LEFT JOIN auth.users     au ON au.id = em.doctor_id
       ${where} ORDER BY em.time DESC LIMIT $${p} OFFSET $${p + 1}`, [...params, limit, offset]);
    const cr = await query(
      `SELECT COUNT(*) AS total FROM ${SCHEMA}.error_medication em
       JOIN ${SCHEMA}.patient pa ON pa.patient_id=em.patient_id
       JOIN ${SCHEMA}.med_table mt ON mt.med_id=em.med_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: ทะเบียนยาหลัก (med_table)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createMedRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const {
      med_name, med_generic_name, med_severity, med_counting_unit,
      med_marketing_name, med_thai_name, med_cost_price, med_selling_price,
      med_medium_price, med_dosage_form, med_medical_category,
      med_essential_med_list, med_replacement, med_TMT_code, med_TPU_code,
      med_dose_dialogue, med_pregnancy_category, med_mfg, med_exp,
    } = req.body;
    if (!med_name || !med_counting_unit || !med_marketing_name || !med_mfg || !med_exp)
      throw new AppError('ข้อมูลจำเป็น: med_name, med_counting_unit, med_marketing_name, med_mfg, med_exp', 400);

    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.med_table
         (med_name, med_generic_name, med_severity, med_counting_unit,
          med_marketing_name, med_thai_name, med_cost_price, med_selling_price,
          med_medium_price, med_dosage_form, med_medical_category,
          med_essential_med_list, med_replacement, "med_TMT_code", "med_TPU_code",
          med_dose_dialogue, med_pregnancy_category, med_mfg, med_exp)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
       RETURNING *`,
      [med_name, med_generic_name, med_severity || 'ยาทั่วไป', med_counting_unit,
        med_marketing_name, med_thai_name, med_cost_price || 0, med_selling_price || 0,
        med_medium_price || 0, med_dosage_form, med_medical_category,
        med_essential_med_list, med_replacement, med_TMT_code, med_TPU_code,
        med_dose_dialogue, med_pregnancy_category, med_mfg, med_exp]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateMedRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id } = req.params;
    const fields = [
      'med_name', 'med_generic_name', 'med_severity', 'med_counting_unit',
      'med_marketing_name', 'med_thai_name', 'med_cost_price', 'med_selling_price',
      'med_medium_price', 'med_dosage_form', 'med_medical_category',
      'med_essential_med_list', 'med_replacement', 'med_pregnancy_category',
      'med_dosage_form', 'med_dose_dialogue', 'med_mfg', 'med_exp',
    ];
    const setClauses: string[] = [];
    const params: any[] = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) {
        setClauses.push(`${f} = $${p}`);
        params.push(req.body[f]);
        p++;
      }
    }
    // quoted fields
    for (const f of ['med_TMT_code', 'med_TPU_code']) {
      if (req.body[f] !== undefined) {
        setClauses.push(`"${f}" = $${p}`);
        params.push(req.body[f]);
        p++;
      }
    }
    if (!setClauses.length) throw new AppError('ไม่มีข้อมูลที่จะอัปเดต', 400);
    params.push(med_id);
    const { rows } = await query(
      `UPDATE ${SCHEMA}.med_table SET ${setClauses.join(', ')}, updated_at = NOW()
       WHERE med_id = $${p} RETURNING *`, params
    );
    if (!rows.length) throw new AppError('ไม่พบทะเบียนยา', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteMedRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id } = req.params;
    // ตรวจว่ายังมีสต็อกอยู่ไหม
    const check = await query(
      `SELECT COUNT(*) AS cnt FROM ${SCHEMA}.med_subwarehouse WHERE med_id = $1 AND med_quantity > 0`, [med_id]
    );
    if (parseInt(check.rows[0].cnt) > 0)
      throw new AppError('ไม่สามารถลบได้ เนื่องจากยังมีสต็อกอยู่ในคลัง', 409);
    const { rowCount } = await query(`DELETE FROM ${SCHEMA}.med_table WHERE med_id = $1`, [med_id]);
    if (!rowCount) throw new AppError('ไม่พบทะเบียนยา', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: การแพ้ยา (allergy_registry)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createAllergy(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id, patient_id, symptoms, description, severity, reported_at } = req.body;
    if (!symptoms) throw new AppError('symptoms จำเป็น', 400);
    const resolvedMed = await resolveMedId(med_id);
    const resolvedPatient = await resolvePatientId(patient_id);
    if (!resolvedMed) throw new AppError('ไม่พบยา (med_id/ชื่อยา)', 400);
    if (!resolvedPatient) throw new AppError('ไม่พบผู้ป่วย (patient_id/HN)', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.allergy_registry (med_id, patient_id, symptoms, description, severity, reported_at)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [resolvedMed, resolvedPatient, symptoms, description, severity || 'mild', reported_at || null]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateAllergy(req: Request, res: Response, next: NextFunction) {
  try {
    const { allr_id } = req.params;
    const { symptoms, description, severity, reported_at } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.allergy_registry
       SET symptoms = COALESCE($1, symptoms), description = COALESCE($2, description),
           severity = COALESCE($3, severity), reported_at = COALESCE($4, reported_at),
           updated_at = NOW()
       WHERE allr_id = $5 RETURNING *`,
      [symptoms, description, severity, reported_at, allr_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteAllergy(req: Request, res: Response, next: NextFunction) {
  try {
    const { allr_id } = req.params;
    const { rowCount } = await query(`DELETE FROM ${SCHEMA}.allergy_registry WHERE allr_id = $1`, [allr_id]);
    if (!rowCount) throw new AppError('ไม่พบรายการ', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: ADR
// ═══════════════════════════════════════════════════════════════════════════════

export async function createAdr(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id, patient_id, description, reported_at, severity, outcome, reporter_id, reporter, symptoms } = req.body;
    if (!description || !reported_at) throw new AppError('description, reported_at จำเป็น', 400);
    // resolve IDs from name/username/hn
    const resolvedMed = await resolveMedId(med_id);
    const resolvedPatient = await resolvePatientId(patient_id);
    const resolvedReporter = (reporter ?? reporter_id) != null
      ? await resolveUserId(reporter ?? reporter_id)
      : ((req as any).currentUser?.id ?? null);
    if (!resolvedMed) throw new AppError('ไม่พบยา (med_id/ชื่อยา)', 400);
    if (!resolvedPatient) throw new AppError('ไม่พบผู้ป่วย (patient_id/HN)', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.adr_registry (med_id, patient_id, description, reported_at, severity, outcome, reporter_id, symptoms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [resolvedMed, resolvedPatient, description, reported_at, severity, outcome, resolvedReporter, symptoms]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateAdr(req: Request, res: Response, next: NextFunction) {
  try {
    const { adr_id } = req.params;
    const { description, severity, outcome, symptoms, notes } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.adr_registry
       SET description = COALESCE($1, description), severity = COALESCE($2, severity),
           outcome = COALESCE($3, outcome), symptoms = COALESCE($4, symptoms), notes = COALESCE($5, notes)
       WHERE adr_id = $6 RETURNING *`,
      [description, severity, outcome, symptoms, notes, adr_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteAdr(req: Request, res: Response, next: NextFunction) {
  try {
    const { adr_id } = req.params;
    const { rowCount } = await query(`DELETE FROM ${SCHEMA}.adr_registry WHERE adr_id = $1`, [adr_id]);
    if (!rowCount) throw new AppError('ไม่พบรายการ', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: ปฏิกิริยายา (med_interaction)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createInteraction(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id_1, med_id_2, description, severity, evidence_level, source_reference, interaction_type } = req.body;
    if (!med_id_1 || !med_id_2 || !description) throw new AppError('med_id_1, med_id_2, description จำเป็น', 400);
    if (med_id_1 === med_id_2) throw new AppError('ไม่สามารถเพิ่มปฏิกิริยากับยาชนิดเดียวกัน', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.med_interaction
         (med_id_1, med_id_2, description, severity, evidence_level, source_reference, interaction_type)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [med_id_1, med_id_2, description, severity, evidence_level, source_reference, interaction_type || 'unknown']
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateInteraction(req: Request, res: Response, next: NextFunction) {
  try {
    const { interaction_id } = req.params;
    const { description, severity, evidence_level, source_reference, interaction_type, is_active } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.med_interaction
       SET description = COALESCE($1, description), severity = COALESCE($2, severity),
           evidence_level = COALESCE($3, evidence_level), source_reference = COALESCE($4, source_reference),
           interaction_type = COALESCE($5, interaction_type), is_active = COALESCE($6, is_active),
           updated_at = NOW()
       WHERE interaction_id = $7 RETURNING *`,
      [description, severity, evidence_level, source_reference, interaction_type, is_active, interaction_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteInteraction(req: Request, res: Response, next: NextFunction) {
  try {
    const { interaction_id } = req.params;
    const { rowCount } = await query(`DELETE FROM ${SCHEMA}.med_interaction WHERE interaction_id = $1`, [interaction_id]);
    if (!rowCount) throw new AppError('ไม่พบรายการ', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: ยาค้างจ่าย (overdue_med)
// ═══════════════════════════════════════════════════════════════════════════════

export async function updateOverdueMed(req: Request, res: Response, next: NextFunction) {
  try {
    const { overdue_id } = req.params;
    const { dispense_status, quantity, note } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.overdue_med
       SET dispense_status = COALESCE($1, dispense_status),
           quantity = COALESCE($2, quantity)
       WHERE overdue_id = $3 RETURNING *`,
      [dispense_status, quantity, overdue_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: RAD (rad_registry)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createRadRequest(req: Request, res: Response, next: NextFunction) {
  try {
    const {
      med_id, med_sid, quantity, unit,
      patient_id, ward,
      diagnosis, infection_site, clinical_indication,
      culture_result, duration_days, prescriber_name,
      requested_by, note,
    } = req.body;
    if (!quantity || !unit)              throw new AppError('quantity, unit จำเป็น', 400);
    if (!diagnosis)                      throw new AppError('diagnosis จำเป็น', 400);
    if (!clinical_indication)            throw new AppError('clinical_indication จำเป็น', 400);
    if (!requested_by)                   throw new AppError('requested_by จำเป็น', 400);
    const resolvedMed = await resolveMedId(med_id);
    if (!resolvedMed)                    throw new AppError('ไม่พบยา', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.rad_registry
         (med_id, med_sid, quantity, unit,
          patient_id, ward,
          diagnosis, infection_site, clinical_indication,
          culture_result, duration_days, prescriber_name,
          requested_by, note, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending')
       RETURNING *`,
      [resolvedMed, med_sid ?? null, quantity, unit,
       patient_id ?? null, ward ?? null,
       diagnosis, infection_site ?? null, clinical_indication,
       culture_result ?? 'not_done', duration_days ?? null, prescriber_name ?? null,
       requested_by, note ?? null]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateRadRequest(req: Request, res: Response, next: NextFunction) {
  try {
    const { rad_id } = req.params;
    const {
      status, approved_by, note, quantity, unit,
      diagnosis, infection_site, clinical_indication,
      culture_result, duration_days, prescriber_name, ward,
    } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.rad_registry
       SET status               = COALESCE($1,  status),
           approved_by          = COALESCE($2,  approved_by),
           approved_time        = CASE WHEN $1 IN ('approved','rejected') THEN NOW() ELSE approved_time END,
           dispensed_time       = CASE WHEN $1 = 'dispensed' THEN NOW() ELSE dispensed_time END,
           note                 = COALESCE($3,  note),
           quantity             = COALESCE($4,  quantity),
           unit                 = COALESCE($5,  unit),
           diagnosis            = COALESCE($6,  diagnosis),
           infection_site       = COALESCE($7,  infection_site),
           clinical_indication  = COALESCE($8,  clinical_indication),
           culture_result       = COALESCE($9,  culture_result),
           duration_days        = COALESCE($10, duration_days),
           prescriber_name      = COALESCE($11, prescriber_name),
           ward                 = COALESCE($12, ward),
           updated_at           = NOW()
       WHERE rad_id = $13 RETURNING *`,
      [status, approved_by ?? null, note, quantity, unit,
       diagnosis, infection_site, clinical_indication,
       culture_result, duration_days, prescriber_name, ward,
       rad_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteRadRequest(req: Request, res: Response, next: NextFunction) {
  try {
    const { rad_id } = req.params;
    const check = await query(`SELECT status FROM ${SCHEMA}.rad_registry WHERE rad_id = $1`, [rad_id]);
    if (!check.rows.length) throw new AppError('ไม่พบรายการ', 404);
    if (!['pending', 'cancelled'].includes(check.rows[0].status))
      throw new AppError('ลบได้เฉพาะรายการที่ pending หรือ cancelled', 409);
    await query(`DELETE FROM ${SCHEMA}.rad_registry WHERE rad_id = $1`, [rad_id]);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers: ค้นหาผู้ป่วย + ค้นหายา (สำหรับ modal forms)
// ═══════════════════════════════════════════════════════════════════════════════

export async function searchPatientsRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { q } = req.query;
    if (!q || String(q).length < 2) { res.json([]); return; }
    const { rows } = await query(
      `SELECT p.patient_id, p.hn_number, p.first_name, p.last_name,
              CONCAT(p.first_name,' ',p.last_name) AS full_name,
              p.national_id, p.phone,
              pa.house_number, pa.village_number, pa.road,
              pa.sub_district, pa.district, pa.province, pa.postal_code
       FROM ${SCHEMA}.patient p
       LEFT JOIN ${SCHEMA}.patient_address pa ON pa.address_id = p.patient_addr_id
       WHERE p.first_name ILIKE $1 OR p.last_name ILIKE $1
          OR p.hn_number ILIKE $1 OR p.national_id ILIKE $1
       ORDER BY p.first_name LIMIT 20`,
      [`%${q}%`]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

export async function searchMedTableRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { q } = req.query;
    if (!q || String(q).length < 1) { res.json([]); return; }
    const { rows } = await query(
      `SELECT med_id, med_name, med_generic_name, med_marketing_name, med_thai_name,
              med_medical_category, med_counting_unit AS unit, med_severity,
              med_dosage_form, med_selling_price
       FROM ${SCHEMA}.med_table
       WHERE med_name ILIKE $1 OR med_generic_name ILIKE $1
          OR med_marketing_name ILIKE $1 OR med_thai_name ILIKE $1
       ORDER BY med_name LIMIT 30`,
      [`%${q}%`]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: med_usage (ประวัติใช้ยา)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createMedUsage(req: Request, res: Response, next: NextFunction) {
  try {
    const { patient_id, med_id, dosage, frequency, route,
      usage_status, start_datetime, end_datetime, note } = req.body;
    const resolvedPatient = await resolvePatientId(patient_id);
    const resolvedMed = await resolveMedId(med_id);
    if (!resolvedPatient) throw new AppError('ไม่พบผู้ป่วย (patient_id/HN)', 400);
    if (!resolvedMed) throw new AppError('ไม่พบยา (med_id/ชื่อยา)', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.med_usage
         (patient_id, med_id, dosage, frequency, route,
          usage_status, start_datetime, end_datetime, note, order_datetime)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) RETURNING *`,
      [resolvedPatient, resolvedMed, dosage, frequency, route,
        usage_status || 'ongoing', start_datetime || null, end_datetime || null, note]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateMedUsage(req: Request, res: Response, next: NextFunction) {
  try {
    const { usage_id } = req.params;
    const { dosage, frequency, route, usage_status, start_datetime, end_datetime, note } = req.body;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.med_usage
       SET dosage          = COALESCE($1, dosage),
           frequency       = COALESCE($2, frequency),
           route           = COALESCE($3, route),
           usage_status    = COALESCE($4, usage_status),
           start_datetime  = COALESCE($5, start_datetime),
           end_datetime    = COALESCE($6, end_datetime),
           note            = COALESCE($7, note),
           updated_at      = NOW()
       WHERE usage_id = $8 RETURNING *`,
      [dosage, frequency, route, usage_status, start_datetime || null, end_datetime || null, note, usage_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: med_delivery (การจัดส่งยา)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createDelivery(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { patient_id, delivery_method, receiver_name, receiver_phone,
      address, note, status, medicine_list,
      courier_name, courier_phone, tracking_number } = req.body;
    if (!delivery_method || !receiver_name || !receiver_phone || !address)
      throw new AppError('delivery_method, receiver_name, receiver_phone, address จำเป็น', 400);
    const resolvedPatient = await resolvePatientId(patient_id);
    if (!resolvedPatient) throw new AppError('ไม่พบผู้ป่วย (patient_id/HN)', 400);

    const medList = Array.isArray(medicine_list) ? medicine_list : [];
    const total_cost = medList.reduce((s: number, m: any) => s + (parseFloat(m.unit_price) || 0) * (parseInt(m.quantity) || 0), 0);

    // ── 1. สร้าง prescription record เบื้องหลัง ──────────────────────────────
    const { rows: prRows } = await client.query(
      `INSERT INTO ${SCHEMA}.prescriptions
         (patient_id, doctor_id, source, ward, note, status)
       VALUES ($1, NULL, 'delivery', NULL, $2, 'pending') RETURNING prescription_id, prescription_no`,
      [resolvedPatient, note || null]
    );
    const { prescription_id, prescription_no } = prRows[0];

    // ── 2. สร้าง prescription_items ──────────────────────────────────────────
    for (const item of medList) {
      await client.query(
        `INSERT INTO ${SCHEMA}.prescription_items
           (prescription_id, med_sid, med_id, quantity)
         VALUES ($1, $2, $3, $4)`,
        [prescription_id, item.med_sid, item.med_id, item.quantity]
      );
    }

    // ── 3. สร้าง med_delivery พร้อม prescription_id ──────────────────────────
    const { rows } = await client.query(
      `INSERT INTO ${SCHEMA}.med_delivery
         (patient_id, delivery_method, receiver_name, receiver_phone,
          address, note, status, medicine_list, delivery_date, total_cost, prescription_id,
          courier_name, courier_phone, tracking_number)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,NOW(),$9,$10,$11,$12,$13) RETURNING *`,
      [resolvedPatient, delivery_method, receiver_name, receiver_phone,
        address, note, status || 'Pending', JSON.stringify(medList), total_cost, prescription_id,
        courier_name || null, courier_phone || null, tracking_number || null]
    );

    await client.query('COMMIT');
    res.status(201).json({ ...rows[0], prescription_no });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

export async function updateDelivery(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { delivery_id } = req.params;
    const { delivery_method, receiver_name, receiver_phone, address, note, status, medicine_list,
      courier_name, courier_phone, tracking_number } = req.body;

    // ── ดึง current record ────────────────────────────────────────────────────
    const { rows: current } = await client.query(
      `SELECT status, prescription_id FROM ${SCHEMA}.med_delivery WHERE delivery_id = $1 FOR UPDATE`,
      [delivery_id]
    );
    if (!current.length) throw new AppError('ไม่พบรายการ', 404);
    const currentStatus: string = current[0].status;
    const prescription_id: number | null = current[0].prescription_id;

    if (currentStatus === 'Delivered' || currentStatus === 'Cancelled')
      throw new AppError('ไม่สามารถแก้ไขได้ สถานะสิ้นสุดแล้ว', 400);

    // ── ตรวจ status transition ────────────────────────────────────────────────
    if (status === 'Delivered' && prescription_id != null) {
      // ดึง prescription + items
      const { rows: prRows } = await client.query(
        `SELECT prescription_no FROM ${SCHEMA}.prescriptions WHERE prescription_id = $1`, [prescription_id]
      );
      const prescription_no = prRows[0]?.prescription_no || null;

      const { rows: items } = await client.query(
        `SELECT item_id, med_sid, med_id, quantity FROM ${SCHEMA}.prescription_items
         WHERE prescription_id = $1`, [prescription_id]
      );

      const resolvedDispenser = ((req as any).currentUser?.id ?? null);

      // ตัดสต็อก FEFO สำหรับแต่ละรายการยา
      for (const item of items) {
        const { rows: drugRows } = await client.query(
          `SELECT med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`, [item.med_sid]
        );
        if (!drugRows.length) throw new AppError(`ไม่พบยา med_sid: ${item.med_sid}`, 404);
        const balBefore = parseInt(drugRows[0].med_quantity);
        if (item.quantity > balBefore)
          throw new AppError(`ยา med_sid:${item.med_sid} มีสต็อกไม่พอ (มี ${balBefore}, ต้องการ ${item.quantity})`, 400);

        const consumed = await deductFefo(item.med_sid, item.quantity, client);
        const balAfter = await recalcTotal(item.med_sid, client);
        const primaryLot = consumed[0];
        await client.query(
          `INSERT INTO ${SCHEMA}.stock_transactions
             (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
              lot_number, expiry_date, prescription_no, performed_by, note)
           VALUES ($1,$2,'out',$3,$4,$5,$6,$7,$8,$9,'จ่ายยาทางการจัดส่ง')`,
          [item.med_sid, item.med_id, item.quantity, balBefore, balAfter,
           primaryLot?.lot_number || null, primaryLot?.exp_date || null,
           prescription_no, resolvedDispenser]
        );
      }

      // mark prescription dispensed
      await client.query(
        `UPDATE ${SCHEMA}.prescriptions
         SET status = 'dispensed', dispensed_at = NOW(), dispensed_by = $1, updated_at = NOW()
         WHERE prescription_id = $2`,
        [resolvedDispenser, prescription_id]
      );
    }

    if (status === 'Cancelled' && prescription_id != null) {
      await client.query(
        `UPDATE ${SCHEMA}.prescriptions
         SET status = 'cancelled', updated_at = NOW()
         WHERE prescription_id = $1 AND status = 'pending'`,
        [prescription_id]
      );
    }

    // ── UPDATE med_delivery ───────────────────────────────────────────────────
    const medList = medicine_list !== undefined ? JSON.stringify(medicine_list) : undefined;
    const total_cost = Array.isArray(medicine_list)
      ? medicine_list.reduce((s: number, m: any) => s + (parseFloat(m.unit_price) || 0) * (parseInt(m.quantity) || 0), 0)
      : undefined;
    const { rows } = await client.query(
      `UPDATE ${SCHEMA}.med_delivery
       SET delivery_method  = COALESCE($1, delivery_method),
           receiver_name    = COALESCE($2, receiver_name),
           receiver_phone   = COALESCE($3, receiver_phone),
           address          = COALESCE($4, address),
           note             = COALESCE($5, note),
           status           = COALESCE($6, status),
           medicine_list    = COALESCE($7::jsonb, medicine_list),
           total_cost       = COALESCE($9, total_cost),
           courier_name     = COALESCE($10, courier_name),
           courier_phone    = COALESCE($11, courier_phone),
           tracking_number  = COALESCE($12, tracking_number),
           delivered_at     = CASE WHEN $6 = 'Delivered' AND delivered_at IS NULL THEN NOW() ELSE delivered_at END
       WHERE delivery_id = $8 RETURNING *`,
      [delivery_method, receiver_name, receiver_phone, address, note, status, medList ?? null, delivery_id, total_cost ?? null,
       courier_name ?? null, courier_phone ?? null, tracking_number ?? null]
    );

    await client.query('COMMIT');
    res.json(rows[0]);
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

export async function deleteDelivery(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { delivery_id } = req.params;
    const { rows } = await client.query(
      `SELECT status, prescription_id FROM ${SCHEMA}.med_delivery WHERE delivery_id = $1`, [delivery_id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);

    if (rows[0].status === 'Delivered')
      throw new AppError('ไม่สามารถลบรายการที่จัดส่งแล้ว', 400);

    // ยกเลิก prescription ที่ยังค้างอยู่
    if (rows[0].prescription_id != null) {
      await client.query(
        `UPDATE ${SCHEMA}.prescriptions SET status = 'cancelled', updated_at = NOW()
         WHERE prescription_id = $1 AND status = 'pending'`,
        [rows[0].prescription_id]
      );
    }

    await client.query(`DELETE FROM ${SCHEMA}.med_delivery WHERE delivery_id = $1`, [delivery_id]);
    await client.query('COMMIT');
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    next(err);
  } finally { client.release(); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRUD: overdue_med (ยาค้างจ่าย)
// ═══════════════════════════════════════════════════════════════════════════════

export async function createOverdue(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id, patient_id, doctor_id, med_sid, quantity } = req.body;
    const resolvedMed = await resolveMedId(med_id);
    const resolvedPatient = await resolvePatientId(patient_id);
    const resolvedDoctor = doctor_id != null
      ? await resolveUserId(doctor_id)
      : ((req as any).currentUser?.id ?? null);
    if (!resolvedMed) throw new AppError('ไม่พบยา (med_id/ชื่อยา)', 400);
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.overdue_med
         (med_id, patient_id, doctor_id, med_sid, quantity, dispense_status, time)
       VALUES ($1,$2,$3,$4,$5,false,NOW()) RETURNING *`,
      [resolvedMed, resolvedPatient || null, resolvedDoctor || null,
        med_sid || null, quantity || null]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateOverdue(req: Request, res: Response, next: NextFunction) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET search_path TO ${SCHEMA}, public`);

    const { overdue_id } = req.params;
    const { dispense_status, quantity, med_id, patient_id, doctor_id, med_sid } = req.body;

    // ดึงรายการเดิมก่อน (ต้องการ med_sid + quantity เมื่อจ่ายยา)
    const { rows: existing } = await client.query(
      `SELECT * FROM ${SCHEMA}.overdue_med WHERE overdue_id = $1 FOR UPDATE`, [overdue_id]
    );
    if (!existing.length) throw new AppError('ไม่พบรายการ', 404);
    const current = existing[0];

    // ถ้ากำลัง mark dispense_status = true (จ่ายยาค้างจ่าย) → หักสต็อก
    if (dispense_status === true && !current.dispense_status) {
      const targetMedSid = med_sid ?? current.med_sid;
      const targetQty    = quantity ?? current.quantity;

      if (!targetMedSid) throw new AppError('ไม่พบ med_sid ของยาค้างจ่าย', 400);

      // ตรวจสต็อกจาก med_subwarehouse (authoritative total)
      const { rows: drugRows } = await client.query(
        `SELECT med_quantity FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1 FOR UPDATE`,
        [targetMedSid]
      );
      if (!drugRows.length) throw new AppError(`ไม่พบยา med_sid: ${targetMedSid}`, 404);

      const balBefore = parseInt(drugRows[0].med_quantity);
      if (targetQty > balBefore)
        throw new AppError(`สต็อกไม่พอ (มี ${balBefore}, ต้องการ ${targetQty})`, 400);

      // sync lot ถ้า med_stock_lots ไม่ครบ (เช่น สต็อกถูกเพิ่มโดยตรง ไม่ผ่านระบบ lot)
      const { rows: lotRows } = await client.query(
        `SELECT COALESCE(SUM(quantity), 0)::int AS lot_total
         FROM ${SCHEMA}.med_stock_lots WHERE med_sid = $1`,
        [targetMedSid]
      );
      const lotTotal = parseInt(lotRows[0].lot_total);
      if (lotTotal < balBefore) {
        await client.query(
          `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, note)
           VALUES ($1, 'SYNC-' || to_char(NOW(), 'YYYYMMDD'), $2, 'ซิงค์สต็อก (auto)')`,
          [targetMedSid, balBefore - lotTotal]
        );
      }

      // FEFO deduction
      const consumed  = await deductFefo(targetMedSid, targetQty, client);
      const balAfter  = await recalcTotal(targetMedSid, client);
      const primaryLot = consumed[0];

      const dispenser = (req as any).currentUser?.id ?? null;

      await client.query(
        `INSERT INTO ${SCHEMA}.stock_transactions
           (med_sid, med_id, tx_type, quantity, balance_before, balance_after,
            lot_number, expiry_date, performed_by, note)
         VALUES ($1,$2,'out',$3,$4,$5,$6,$7,$8,'จ่ายยาค้างจ่าย')`,
        [targetMedSid, current.med_id, targetQty, balBefore, balAfter,
         primaryLot?.lot_number || null, primaryLot?.exp_date || null, dispenser]
      );
    }

    const resolvedMed     = med_id     ? await resolveMedId(med_id)         : undefined;
    const resolvedPatient = patient_id ? await resolvePatientId(patient_id) : undefined;
    const resolvedDoctor  = doctor_id  ? await resolveUserId(doctor_id)     : undefined;

    const { rows } = await client.query(
      `UPDATE ${SCHEMA}.overdue_med
       SET dispense_status = COALESCE($1, dispense_status),
           quantity        = COALESCE($2, quantity),
           med_id          = COALESCE($3, med_id),
           patient_id      = COALESCE($4, patient_id),
           doctor_id       = COALESCE($5, doctor_id),
           med_sid         = COALESCE($6, med_sid)
       WHERE overdue_id = $7 RETURNING *`,
      [dispense_status ?? null, quantity ?? null,
       resolvedMed ?? null, resolvedPatient ?? null,
       resolvedDoctor ?? null, med_sid ?? null,
       overdue_id]
    );

    await client.query('COMMIT');
    res.json(rows[0]);
  } catch (err) {
    await client.query('ROLLBACK');
    next(err);
  } finally { client.release(); }
}

export async function deleteOverdue(req: Request, res: Response, next: NextFunction) {
  try {
    const { overdue_id } = req.params;
    const { rowCount } = await query(
      `DELETE FROM ${SCHEMA}.overdue_med WHERE overdue_id = $1`, [overdue_id]
    );
    if (!rowCount) throw new AppError('ไม่พบรายการ', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ═══ PATIENTS ════════════════════════════════════════════════════════════════

export async function getPatients(req: Request, res: Response, next: NextFunction) {
  try {
    const { search } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (first_name ILIKE $${p} OR last_name ILIKE $${p} OR hn_number ILIKE $${p} OR national_id ILIKE $${p} OR phone ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    const { rows } = await query(
      `SELECT *, CONCAT(first_name,' ',last_name) AS full_name
       FROM ${SCHEMA}.patient ${where}
       ORDER BY first_name LIMIT $${p} OFFSET $${p+1}`,
      [...params, limit, offset]
    );
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.patient ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

export async function getPatientById(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const [patR, allergyR, adrR] = await Promise.all([
      query(
        `SELECT p.*,
           CONCAT(p.first_name,' ',p.last_name) AS full_name,
           pa.house_number, pa.village_number, pa.road,
           pa.sub_district, pa.district, pa.province, pa.postal_code
         FROM ${SCHEMA}.patient p
         LEFT JOIN ${SCHEMA}.patient_address pa ON pa.address_id = p.patient_addr_id
         WHERE p.patient_id=$1`,
        [id]
      ),
      query(`SELECT ar.*, mt.med_name, mt.med_generic_name FROM ${SCHEMA}.allergy_registry ar JOIN ${SCHEMA}.med_table mt ON mt.med_id=ar.med_id WHERE ar.patient_id=$1 ORDER BY ar.reported_at DESC LIMIT 10`, [id]),
      query(`SELECT adr.*, mt.med_name FROM ${SCHEMA}.adr_registry adr JOIN ${SCHEMA}.med_table mt ON mt.med_id=adr.med_id WHERE adr.patient_id=$1 ORDER BY adr.reported_at DESC LIMIT 10`, [id]),
    ]);
    if (!patR.rows.length) throw new AppError('ไม่พบผู้ป่วย', 404);
    res.json({ patient: patR.rows[0], allergies: allergyR.rows, adrs: adrR.rows });
  } catch (err) { next(err); }
}

// ═══ MED PROBLEM ═════════════════════════════════════════════════════════════

export async function getMedProblems(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, resolved, type } = req.query;
    const { limit, offset } = paginate(req.query.page, req.query.limit);
    const params: any[] = []; let w = 'WHERE 1=1'; let p = 1;
    if (search) {
      w += ` AND (mt.med_name ILIKE $${p} OR mt.med_generic_name ILIKE $${p} OR mp.description ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (resolved !== undefined) { w += ` AND mp.is_resolved=$${p}`; params.push(resolved === 'true'); p++; }
    if (type) { w += ` AND mp.problem_type=$${p}`; params.push(type); p++; }
    const { rows } = await query(
      `SELECT mp.*, mt.med_name, mt.med_generic_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS reported_by_name,
              CONCAT(pa.first_name, ' ', pa.last_name) AS patient_name, pa.hn_number
       FROM ${SCHEMA}.med_problem mp
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = mp.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = mp.patient_id
       LEFT JOIN public.profiles pu ON pu.id = mp.reported_by
       LEFT JOIN auth.users     au ON au.id = mp.reported_by
       ${w} ORDER BY mp.reported_at DESC NULLS LAST
       LIMIT $${p} OFFSET $${p+1}`,
      [...params, limit, offset]
    );
    const cr = await query(
      `SELECT COUNT(*) AS total FROM ${SCHEMA}.med_problem mp
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = mp.med_id
       LEFT JOIN ${SCHEMA}.patient pa ON pa.patient_id = mp.patient_id
       ${w}`, params
    );
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

export async function createMedProblem(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id, patient_id, problem_type, description, is_resolved, reported_by, reported_at } = req.body;
    if (!med_id)       throw new AppError('กรุณาเลือกยา', 400);
    if (!problem_type) throw new AppError('กรุณาระบุประเภทปัญหา', 400);
    if (!description)  throw new AppError('กรุณากรอกคำอธิบาย', 400);
    const resolvedBy = reported_by ? await resolveUserId(reported_by) : null;
    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.med_problem
         (med_id, patient_id, problem_type, description, is_resolved, reported_by, reported_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [med_id, patient_id || null, problem_type, description,
       is_resolved ?? false, resolvedBy, reported_at || new Date()]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

export async function updateMedProblem(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { med_id, patient_id, problem_type, description, is_resolved, reported_by, reported_at } = req.body;
    const resolvedBy = reported_by ? await resolveUserId(reported_by) : null;
    const { rows } = await query(
      `UPDATE ${SCHEMA}.med_problem SET
         med_id       = COALESCE($1, med_id),
         patient_id   = COALESCE($2, patient_id),
         problem_type = COALESCE($3, problem_type),
         description  = COALESCE($4, description),
         is_resolved  = COALESCE($5, is_resolved),
         reported_by  = COALESCE($6, reported_by),
         reported_at  = COALESCE($7, reported_at)
       WHERE problem_id = $8 RETURNING *`,
      [med_id, patient_id || null, problem_type, description,
       is_resolved, resolvedBy, reported_at, id]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการ', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

export async function deleteMedProblem(req: Request, res: Response, next: NextFunction) {
  try {
    const { id } = req.params;
    const { rowCount } = await query(
      `DELETE FROM ${SCHEMA}.med_problem WHERE problem_id = $1`, [id]
    );
    if (!rowCount) throw new AppError('ไม่พบรายการ', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}
