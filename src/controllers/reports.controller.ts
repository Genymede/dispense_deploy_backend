import { Request, Response, NextFunction } from 'express';
import { query, SCHEMA } from '../db/pool';
import { AppError } from '../middleware/errorHandler';
import ExcelJS from 'exceljs';
import PDFDocument from 'pdfkit';
import { format } from 'date-fns';
import { th } from 'date-fns/locale';
import * as path from 'path';
import * as fs from 'fs';

// ─── Font / logo paths ─────────────────────────────────────────────────────────
const FONT_DIR  = path.resolve(process.cwd(), '../../pharmacy-system-v19/pharmacy-system/app/public/font/ThaiSarabun');
const FONT_REG  = path.join(FONT_DIR, 'subset-Sarabun-Regular.ttf');
const FONT_BOLD = path.join(FONT_DIR, 'subset-Sarabun-Bold.ttf');
const LOGO_PATH = path.resolve(process.cwd(), '../../pharmacy-system-v19/pharmacy-system/app/public/logo.png');

const HOSPITAL = {
  name:    'โรงพยาบาลวัดห้วยปลากั้งเพื่อสังคม',
  address: '553 11 ตำบล บ้านดู่ อำเภอเมืองเชียงราย เชียงราย 57100',
  phone:   '052 029 888',
};

// ─── helpers ──────────────────────────────────────────────────────────────────

function dateParam(v: unknown): string | undefined {
  return typeof v === 'string' && v ? v : undefined;
}

function thDate(d: Date | string | null): string {
  if (!d) return '-';
  const date = typeof d === 'string' ? new Date(d) : d;
  return format(date, 'd MMM yyyy', { locale: th });
}

// Excel: สร้าง header row พร้อม style
function excelHeader(ws: ExcelJS.Worksheet, cols: { header: string; key: string; width: number }[]) {
  ws.columns = cols;
  const hRow = ws.getRow(1);
  hRow.font      = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
  hRow.fill      = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF006FC6' } };
  hRow.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
  hRow.height    = 22;
}

// Excel: alternate row shading
function excelRows(ws: ExcelJS.Worksheet, data: any[], keys: string[]) {
  data.forEach((row, i) => {
    const r = ws.addRow(keys.map(k => row[k] ?? '-'));
    if (i % 2 === 1) r.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F7FF' } };
    r.alignment = { vertical: 'middle', wrapText: false };
  });
}

// PDF: hospital header with Sarabun font
function pdfHeader(doc: InstanceType<typeof PDFDocument>, title: string, subtitle?: string) {
  const W = doc.page.width;
  const margin = 40;

  // Logo (top-left)
  if (fs.existsSync(LOGO_PATH)) {
    doc.image(LOGO_PATH, margin, margin, { width: 50, height: 50 });
  }

  const CW = W - 2 * margin; // centered text width

  // Hospital info (centered)
  doc.font('Sarabun-Bold').fontSize(14).text(HOSPITAL.name,    margin, margin, { width: CW, align: 'center' });
  doc.font('Sarabun').fontSize(9) .text(HOSPITAL.address,                      { width: CW, align: 'center' });
  doc.text(`โทร: ${HOSPITAL.phone}`,                                            { width: CW, align: 'center' });
  doc.text(`วันที่: ${format(new Date(), 'd MMMM yyyy', { locale: th })}`,      { width: CW, align: 'center' });

  // Report title
  doc.moveDown(0.4);
  doc.font('Sarabun-Bold').fontSize(12).text(title, { width: CW, align: 'center' });
  if (subtitle) { doc.font('Sarabun').fontSize(9).text(subtitle, { width: CW, align: 'center' }); }

  // Divider
  doc.moveDown(0.5);
  doc.moveTo(margin, doc.y).lineTo(W - margin, doc.y).stroke('#006FC6');
  doc.moveDown(0.5);
}

// ─── GET /reports/stock ────────────────────────────────────────────────────────
export async function getStockReport(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to, tx_type } = req.query;
    if (!date_from || !date_to) throw new AppError('date_from และ date_to จำเป็น', 400);
    const params: any[] = [date_from, date_to];
    let typeFilter = '';
    if (tx_type) { typeFilter = ` AND st.tx_type = $3`; params.push(tx_type); }

    const { rows } = await query(
      `SELECT st.*,
              COALESCE(ms.med_showname, mt.med_name) AS drug_name,
              mt.med_generic_name, mt.med_medical_category AS category,
              mt.med_counting_unit AS unit,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS performed_by_name
       FROM   ${SCHEMA}.stock_transactions st
       JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
       JOIN   ${SCHEMA}.med_table        mt ON mt.med_id  = st.med_id
       LEFT JOIN public.profiles         pu ON pu.id      = st.performed_by
       LEFT JOIN auth.users              au ON au.id      = st.performed_by
       WHERE  st.created_at >= $1 AND st.created_at < $2::date + 1
              ${typeFilter}
       ORDER  BY st.created_at DESC
       LIMIT 5000`, params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/dispense ─────────────────────────────────────────────────────
export async function getDispenseReport(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to, ward } = req.query;
    if (!date_from || !date_to) throw new AppError('date_from และ date_to จำเป็น', 400);
    const params: any[] = [date_from, date_to];
    let wardF = '';
    if (ward) { wardF = ` AND pr.ward = $3`; params.push(ward); }

    const { rows } = await query(
      `SELECT pr.*,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name,
              pa.hn_number,
              COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
              COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name,
              COUNT(pi.item_id)  AS item_count,
              SUM(pi.quantity)   AS total_quantity
       FROM   ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.patient    pa    ON pa.patient_id  = pr.patient_id
       LEFT JOIN public.profiles      pdoc  ON pdoc.id        = pr.doctor_id
       LEFT JOIN auth.users           adoc  ON adoc.id        = pr.doctor_id
       LEFT JOIN public.profiles      pdisp ON pdisp.id       = pr.dispensed_by
       LEFT JOIN auth.users           adisp ON adisp.id       = pr.dispensed_by
       LEFT JOIN ${SCHEMA}.prescription_items pi ON pi.prescription_id = pr.prescription_id
       WHERE  pr.created_at >= $1 AND pr.created_at < $2::date + 1 ${wardF}
       GROUP  BY pr.prescription_id, pa.first_name, pa.last_name, pa.hn_number,
                 pdoc.firstname_th, pdoc.lastname_th, adoc.email,
                 pdisp.firstname_th, pdisp.lastname_th, adisp.email
       ORDER  BY pr.created_at DESC
       LIMIT 5000`, params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/inventory ────────────────────────────────────────────────────
export async function getInventoryReport(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(
      `SELECT ms.med_sid, ms.med_quantity AS current_stock,
              ms.min_quantity, ms.max_quantity, ms.exp_date, ms.mfg_date,
              ms.is_expired, ms.cost_price, ms.unit_price, ms.location,
              ms.packaging_type,
              COALESCE(ms.med_showname, mt.med_name)  AS drug_name,
              mt.med_generic_name, mt.med_medical_category AS category,
              mt.med_counting_unit AS unit,
              mt.med_marketing_name,
              (ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_value,
              CASE
                WHEN ms.is_expired OR ms.exp_date < NOW()                THEN 'expired'
                WHEN ms.med_quantity = 0                                  THEN 'out_of_stock'
                WHEN ms.min_quantity IS NOT NULL
                 AND ms.med_quantity < ms.min_quantity                    THEN 'low_stock'
                ELSE                                                           'normal'
              END AS stock_status
       FROM   ${SCHEMA}.med_subwarehouse ms
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       ORDER  BY drug_name`
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/top-drugs ────────────────────────────────────────────────────
export async function getTopDrugs(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to, limit = 10 } = req.query;
    const params: any[] = [Number(limit)];
    let df = '';
    if (date_from) { df += ` AND st.created_at >= $${params.length + 1}`; params.push(date_from); }
    if (date_to)   { df += ` AND st.created_at < $${params.length + 1}::date + 1`; params.push(date_to); }

    const { rows } = await query(
      `SELECT ms.med_sid,
              COALESCE(ms.med_showname, mt.med_name) AS drug_name,
              mt.med_medical_category AS category, mt.med_counting_unit AS unit,
              SUM(ABS(st.quantity)) AS total_dispensed,
              SUM(ABS(st.quantity) * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_value
       FROM   ${SCHEMA}.stock_transactions st
       JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
       JOIN   ${SCHEMA}.med_table        mt ON mt.med_id  = st.med_id
       WHERE  st.tx_type = 'out' ${df}
       GROUP  BY ms.med_sid, drug_name, mt.med_medical_category, mt.med_counting_unit
       ORDER  BY total_dispensed DESC LIMIT $1`, params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/by-category ──────────────────────────────────────────────────
export async function getByCategory(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to } = req.query;
    const params: any[] = [];
    let df = '';
    if (date_from) { df += ` AND st.created_at >= $${params.length + 1}`; params.push(date_from); }
    if (date_to)   { df += ` AND st.created_at < $${params.length + 1}::date + 1`; params.push(date_to); }

    const { rows } = await query(
      `SELECT COALESCE(mt.med_medical_category,'ไม่ระบุ') AS category,
              SUM(ABS(st.quantity)) AS total_dispensed,
              COUNT(DISTINCT st.med_sid) AS drug_count
       FROM   ${SCHEMA}.stock_transactions st
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id = st.med_id
       WHERE  st.tx_type = 'out' ${df}
       GROUP  BY mt.med_medical_category ORDER BY total_dispensed DESC`, params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/by-ward ──────────────────────────────────────────────────────
export async function getByWard(req: Request, res: Response, next: NextFunction) {
  try {
    const { date_from, date_to } = req.query;
    const params: any[] = [];
    let df = '';
    if (date_from) { df += ` AND pr.created_at >= $${params.length + 1}`; params.push(date_from); }
    if (date_to)   { df += ` AND pr.created_at < $${params.length + 1}::date + 1`; params.push(date_to); }

    const { rows } = await query(
      `SELECT COALESCE(pr.ward,'ไม่ระบุ') AS ward,
              COUNT(pr.prescription_id) AS prescription_count,
              COALESCE(SUM(pi.quantity), 0) AS total_quantity
       FROM   ${SCHEMA}.prescriptions pr
       LEFT JOIN ${SCHEMA}.prescription_items pi ON pi.prescription_id = pr.prescription_id
       WHERE  pr.status IN ('dispensed','returned') ${df}
       GROUP  BY pr.ward ORDER BY prescription_count DESC`, params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ─── GET /reports/registry ─────────────────────────────────────────────────────
// ทะเบียนยาหลัก (med_table)
export async function getMedRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, category, page = 1, limit = 100 } = req.query;
    const params: any[] = [];
    let where = 'WHERE 1=1';
    let p = 1;
    if (search) {
      where += ` AND (med_name ILIKE $${p} OR med_generic_name ILIKE $${p} OR med_marketing_name ILIKE $${p} OR "med_TMT_code" ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (category) { where += ` AND med_medical_category = $${p}`; params.push(category); p++; }

    const offset = (Number(page) - 1) * Number(limit);
    const { rows } = await query(
      `SELECT * FROM ${SCHEMA}.med_table ${where}
       ORDER BY med_name LIMIT $${p} OFFSET $${p + 1}`,
      [...params, Number(limit), offset]
    );
    const countRes = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.med_table ${where}`, params);
    res.json({ data: rows, total: parseInt(countRes.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ─── GET /reports/registry/:med_id ────────────────────────────────────────────
export async function getMedRegistryById(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_id } = req.params;
    const [medRes, subRes, interRes, allerRes] = await Promise.all([
      query(`SELECT * FROM ${SCHEMA}.med_table WHERE med_id = $1`, [med_id]),
      query(`SELECT * FROM ${SCHEMA}.med_subwarehouse WHERE med_id = $1 ORDER BY med_sid`, [med_id]),
      query(
        `SELECT mi.*, mt2.med_name AS interacts_with_name
         FROM ${SCHEMA}.med_interaction mi
         JOIN ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
         WHERE mi.med_id_1 = $1 AND mi.is_active = true`, [med_id]
      ),
      query(
        `SELECT ar.*, CONCAT(p.first_name,' ',p.last_name) AS patient_name, p.hn_number
         FROM ${SCHEMA}.allergy_registry ar
         JOIN ${SCHEMA}.patient p ON p.patient_id = ar.patient_id
         WHERE ar.med_id = $1 ORDER BY ar.reported_at DESC LIMIT 20`, [med_id]
      ),
    ]);
    if (!medRes.rows.length) throw new AppError('ไม่พบทะเบียนยา', 404);
    res.json({
      med: medRes.rows[0],
      subwarehouse: subRes.rows,
      interactions: interRes.rows,
      allergies: allerRes.rows,
    });
  } catch (err) { next(err); }
}

// ─── GET /reports/allergy ──────────────────────────────────────────────────────
export async function getAllergyRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, severity, page = 1, limit = 50 } = req.query;
    const params: any[] = [];
    let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    if (severity) { where += ` AND ar.severity = $${p}`; params.push(severity); p++; }

    const offset = (Number(page) - 1) * Number(limit);
    const { rows } = await query(
      `SELECT ar.*,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name, mt.med_generic_name
       FROM   ${SCHEMA}.allergy_registry ar
       JOIN   ${SCHEMA}.patient   pa ON pa.patient_id = ar.patient_id
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id     = ar.med_id
       ${where} ORDER BY ar.created_at DESC
       LIMIT $${p} OFFSET $${p + 1}`, [...params, Number(limit), offset]
    );
    const cr = await query(`SELECT COUNT(*) AS total FROM ${SCHEMA}.allergy_registry ar JOIN ${SCHEMA}.patient pa ON pa.patient_id=ar.patient_id JOIN ${SCHEMA}.med_table mt ON mt.med_id=ar.med_id ${where}`, params);
    res.json({ data: rows, total: parseInt(cr.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ─── GET /reports/adr ─────────────────────────────────────────────────────────
export async function getAdrRegistry(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, page = 1, limit = 50 } = req.query;
    const params: any[] = [];
    let where = 'WHERE 1=1'; let p = 1;
    if (search) {
      where += ` AND (CONCAT(pa.first_name,' ',pa.last_name) ILIKE $${p} OR pa.hn_number ILIKE $${p} OR mt.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    const offset = (Number(page) - 1) * Number(limit);
    const { rows } = await query(
      `SELECT adr.*,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS reporter_name
       FROM   ${SCHEMA}.adr_registry adr
       JOIN   ${SCHEMA}.patient    pa ON pa.patient_id = adr.patient_id
       JOIN   ${SCHEMA}.med_table  mt ON mt.med_id     = adr.med_id
       LEFT JOIN public.profiles   pu ON pu.id         = adr.reporter_id
       LEFT JOIN auth.users        au ON au.id         = adr.reporter_id
       ${where} ORDER BY adr.reported_at DESC
       LIMIT $${p} OFFSET $${p + 1}`, [...params, Number(limit), offset]
    );
    res.json({ data: rows, total: rows.length });
  } catch (err) { next(err); }
}

// ─── GET /reports/med-error ────────────────────────────────────────────────────
export async function getMedError(req: Request, res: Response, next: NextFunction) {
  try {
    const { resolved, page = 1, limit = 50 } = req.query;
    const params: any[] = [];
    let where = 'WHERE 1=1'; let p = 1;
    if (resolved !== undefined) { where += ` AND em.resolved = $${p}`; params.push(resolved === 'true'); p++; }
    const offset = (Number(page) - 1) * Number(limit);
    const { rows } = await query(
      `SELECT em.*,
              CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
              mt.med_name,
              COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name
       FROM   ${SCHEMA}.error_medication em
       JOIN   ${SCHEMA}.patient   pa ON pa.patient_id = em.patient_id
       JOIN   ${SCHEMA}.med_table mt ON mt.med_id     = em.med_id
       LEFT JOIN public.profiles  pu ON pu.id         = em.doctor_id
       LEFT JOIN auth.users       au ON au.id         = em.doctor_id
       ${where} ORDER BY em.time DESC
       LIMIT $${p} OFFSET $${p + 1}`, [...params, Number(limit), offset]
    );
    res.json({ data: rows, total: rows.length });
  } catch (err) { next(err); }
}

// ─── GET /reports/interactions ─────────────────────────────────────────────────
export async function getMedInteractions(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, type, page = 1, limit = 50 } = req.query;
    const params: any[] = [];
    let where = 'WHERE mi.is_active = true'; let p = 1;
    if (type) { where += ` AND mi.interaction_type = $${p}`; params.push(type); p++; }
    if (search) {
      where += ` AND (mt1.med_name ILIKE $${p} OR mt2.med_name ILIKE $${p})`;
      params.push(`%${search}%`); p++;
    }
    const offset = (Number(page) - 1) * Number(limit);
    const { rows } = await query(
      `SELECT mi.*,
              mt1.med_name AS drug1_name, mt2.med_name AS drug2_name
       FROM   ${SCHEMA}.med_interaction mi
       JOIN   ${SCHEMA}.med_table mt1 ON mt1.med_id = mi.med_id_1
       JOIN   ${SCHEMA}.med_table mt2 ON mt2.med_id = mi.med_id_2
       ${where} ORDER BY mi.severity DESC, mt1.med_name
       LIMIT $${p} OFFSET $${p + 1}`, [...params, Number(limit), offset]
    );
    res.json({ data: rows, total: rows.length });
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORT — Excel
// ═══════════════════════════════════════════════════════════════════════════════

export async function exportExcel(req: Request, res: Response, next: NextFunction) {
  try {
    const { report, date_from, date_to, ward, tx_type } = req.query as Record<string, string>;
    const wb = new ExcelJS.Workbook();
    wb.creator  = 'PharmSub';
    wb.created  = new Date();
    wb.modified = new Date();

    const dateLabel = date_from && date_to
      ? `${thDate(date_from)} – ${thDate(date_to)}`
      : thDate(new Date());

    // ── inventory ─────────────────────────────────────────────
    if (report === 'inventory') {
      const { rows } = await query(
        `SELECT COALESCE(ms.med_showname, mt.med_name) AS drug_name,
                mt.med_generic_name, mt.med_medical_category AS category,
                mt.med_counting_unit AS unit, ms.packaging_type,
                ms.med_quantity AS current_stock, ms.min_quantity, ms.max_quantity,
                ms.location, ms.exp_date, ms.is_expired,
                ms.cost_price, ms.unit_price,
                (ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_value,
                CASE WHEN ms.is_expired OR ms.exp_date < NOW() THEN 'หมดอายุ'
                     WHEN ms.med_quantity = 0 THEN 'หมดสต็อก'
                     WHEN ms.min_quantity IS NOT NULL AND ms.med_quantity < ms.min_quantity THEN 'ต่ำ'
                     ELSE 'ปกติ' END AS status
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         ORDER BY drug_name`
      );
      const ws = wb.addWorksheet('คงคลัง');
      excelHeader(ws, [
        { header: 'ชื่อยา',         key: 'drug_name',      width: 36 },
        { header: 'ชื่อสามัญ',       key: 'med_generic_name', width: 28 },
        { header: 'หมวดหมู่',        key: 'category',       width: 24 },
        { header: 'หน่วย',           key: 'unit',           width: 10 },
        { header: 'รูปแบบ',          key: 'packaging_type', width: 12 },
        { header: 'คงเหลือ',         key: 'current_stock',  width: 10 },
        { header: 'ขั้นต่ำ',          key: 'min_quantity',   width: 10 },
        { header: 'สูงสุด',           key: 'max_quantity',   width: 10 },
        { header: 'ที่เก็บ',          key: 'location',       width: 10 },
        { header: 'วันหมดอายุ',      key: 'exp_date',       width: 14 },
        { header: 'ราคาต้นทุน',      key: 'cost_price',     width: 14 },
        { header: 'ราคาขาย',         key: 'unit_price',     width: 12 },
        { header: 'มูลค่าคงคลัง',    key: 'total_value',    width: 16 },
        { header: 'สถานะ',           key: 'status',         width: 12 },
      ]);
      rows.forEach((r, i) => {
        const row = ws.addRow([
          r.drug_name, r.med_generic_name, r.category, r.unit, r.packaging_type,
          r.current_stock, r.min_quantity, r.max_quantity, r.location,
          r.exp_date ? thDate(r.exp_date) : '-',
          r.cost_price, r.unit_price,
          Number(r.total_value).toFixed(2), r.status,
        ]);
        if (i % 2 === 1) row.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF0F7FF' } };
        if (r.status !== 'ปกติ') {
          row.getCell(14).font = { bold: true, color: { argb: r.status === 'หมดอายุ' ? 'FFDC2626' : 'FFD97706' } };
        }
      });
      // summary row
      ws.addRow([]);
      const total = rows.reduce((s, r) => s + Number(r.total_value || 0), 0);
      const sumRow = ws.addRow(['', '', '', '', '', '', '', '', '', '', '', 'รวมมูลค่า:', total.toFixed(2), '']);
      sumRow.font = { bold: true }; sumRow.getCell(13).numFmt = '#,##0.00';
    }

    // ── stock ─────────────────────────────────────────────────
    else if (report === 'stock') {
      const params: any[] = [date_from, date_to];
      let tf = ''; if (tx_type) { tf = ` AND st.tx_type = $3`; params.push(tx_type); }
      const { rows } = await query(
        `SELECT TO_CHAR(st.created_at,'DD/MM/YYYY HH24:MI') AS tx_time,
                st.tx_type,
                COALESCE(ms.med_showname, mt.med_name) AS drug_name,
                mt.med_generic_name, mt.med_counting_unit AS unit,
                st.quantity, st.balance_before, st.balance_after,
                st.lot_number, st.reference_no, st.prescription_no,
                st.ward_from, st.note,
                COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS performed_by
         FROM   ${SCHEMA}.stock_transactions st
         JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
         JOIN   ${SCHEMA}.med_table        mt ON mt.med_id  = st.med_id
         LEFT JOIN public.profiles         pu ON pu.id      = st.performed_by
         LEFT JOIN auth.users              au ON au.id      = st.performed_by
         WHERE  st.created_at >= $1 AND st.created_at < $2::date + 1 ${tf}
         ORDER  BY st.created_at DESC`, params
      );
      const txLabel: Record<string, string> = { in: 'รับเข้า', out: 'จ่ายออก', adjust: 'ปรับสต็อก', return: 'คืนยา', expired: 'หมดอายุ' };
      const ws = wb.addWorksheet('ความเคลื่อนไหว');
      excelHeader(ws, [
        { header: 'วันเวลา',     key: 'tx_time',        width: 18 },
        { header: 'ประเภท',     key: 'tx_type',         width: 12 },
        { header: 'ชื่อยา',     key: 'drug_name',       width: 34 },
        { header: 'ชื่อสามัญ',  key: 'med_generic_name', width: 26 },
        { header: 'หน่วย',      key: 'unit',            width: 10 },
        { header: 'จำนวน',      key: 'quantity',        width: 10 },
        { header: 'สต็อกก่อน', key: 'balance_before',  width: 12 },
        { header: 'สต็อกหลัง', key: 'balance_after',   width: 12 },
        { header: 'Lot',        key: 'lot_number',      width: 16 },
        { header: 'เลขอ้างอิง', key: 'reference_no',   width: 16 },
        { header: 'เลขใบสั่งยา', key: 'prescription_no', width: 18 },
        { header: 'วอร์ด',     key: 'ward_from',       width: 14 },
        { header: 'ผู้บันทึก',  key: 'performed_by',   width: 16 },
        { header: 'หมายเหตุ',  key: 'note',            width: 28 },
      ]);
      excelRows(ws, rows.map(r => ({ ...r, tx_type: txLabel[r.tx_type] ?? r.tx_type })),
        ['tx_time','tx_type','drug_name','med_generic_name','unit','quantity',
         'balance_before','balance_after','lot_number','reference_no','prescription_no',
         'ward_from','performed_by','note']);
    }

    // ── dispense ──────────────────────────────────────────────
    else if (report === 'dispense') {
      const params: any[] = [date_from, date_to];
      let wf = ''; if (ward) { wf = ` AND pr.ward = $3`; params.push(ward); }
      const { rows } = await query(
        `SELECT pr.prescription_no,
                TO_CHAR(pr.created_at,'DD/MM/YYYY HH24:MI') AS created_time,
                CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
                pr.ward,
                COALESCE(pdoc.firstname_th || ' ' || pdoc.lastname_th, adoc.email, '') AS doctor_name,
                pr.status,
                TO_CHAR(pr.dispensed_at,'DD/MM/YYYY HH24:MI') AS dispensed_time,
                COALESCE(pdisp.firstname_th || ' ' || pdisp.lastname_th, adisp.email, '') AS dispensed_by_name,
                COUNT(pi.item_id)::INTEGER AS item_count,
                SUM(pi.quantity)::INTEGER  AS total_quantity, pr.note
         FROM   ${SCHEMA}.prescriptions pr
         LEFT JOIN ${SCHEMA}.patient    pa    ON pa.patient_id  = pr.patient_id
         LEFT JOIN public.profiles      pdoc  ON pdoc.id        = pr.doctor_id
         LEFT JOIN auth.users           adoc  ON adoc.id        = pr.doctor_id
         LEFT JOIN public.profiles      pdisp ON pdisp.id       = pr.dispensed_by
         LEFT JOIN auth.users           adisp ON adisp.id       = pr.dispensed_by
         LEFT JOIN ${SCHEMA}.prescription_items pi ON pi.prescription_id = pr.prescription_id
         WHERE  pr.created_at >= $1 AND pr.created_at < $2::date + 1 ${wf}
         GROUP  BY pr.prescription_id, pa.first_name, pa.last_name, pa.hn_number,
                   pdoc.firstname_th, pdoc.lastname_th, adoc.email,
                   pdisp.firstname_th, pdisp.lastname_th, adisp.email
         ORDER  BY pr.created_at DESC`, params
      );
      const statusTh: Record<string, string> = { pending: 'รอจ่าย', dispensed: 'จ่ายแล้ว', returned: 'คืนยา', cancelled: 'ยกเลิก' };
      const ws = wb.addWorksheet('จ่ายยา');
      excelHeader(ws, [
        { header: 'เลขใบสั่ง',   key: 'prescription_no',  width: 20 },
        { header: 'วันที่สั่ง',   key: 'created_time',     width: 18 },
        { header: 'ผู้ป่วย',      key: 'patient_name',     width: 28 },
        { header: 'HN',          key: 'hn_number',         width: 14 },
        { header: 'วอร์ด',       key: 'ward',              width: 16 },
        { header: 'แพทย์',       key: 'doctor_name',       width: 18 },
        { header: 'สถานะ',       key: 'status',            width: 12 },
        { header: 'วันที่จ่าย',  key: 'dispensed_time',   width: 18 },
        { header: 'ผู้จ่าย',     key: 'dispensed_by_name', width: 18 },
        { header: 'รายการยา',    key: 'item_count',        width: 10 },
        { header: 'จำนวนรวม',   key: 'total_quantity',    width: 12 },
        { header: 'หมายเหตุ',   key: 'note',               width: 28 },
      ]);
      excelRows(ws, rows.map(r => ({ ...r, status: statusTh[r.status] ?? r.status })),
        ['prescription_no','created_time','patient_name','hn_number','ward','doctor_name',
         'status','dispensed_time','dispensed_by_name','item_count','total_quantity','note']);
    }

    // ── registry ──────────────────────────────────────────────
    else if (report === 'registry') {
      const { rows } = await query(
        `SELECT med_id, med_name, med_generic_name, med_marketing_name, med_thai_name,
                med_medical_category, med_dosage_form, med_counting_unit,
                med_severity, med_cost_price, med_selling_price,
                med_essential_med_list, med_pregnancy_category,
                "med_TMT_code", "med_TPU_code",
                TO_CHAR(med_mfg,'DD/MM/YYYY') AS mfg_date,
                TO_CHAR(med_exp,'DD/MM/YYYY') AS exp_date
         FROM ${SCHEMA}.med_table ORDER BY med_name`
      );
      const ws = wb.addWorksheet('ทะเบียนยา');
      excelHeader(ws, [
        { header: 'รหัส',           key: 'med_id',               width: 8 },
        { header: 'ชื่อยา',         key: 'med_name',             width: 34 },
        { header: 'ชื่อสามัญ',      key: 'med_generic_name',     width: 28 },
        { header: 'ชื่อการค้า',     key: 'med_marketing_name',   width: 26 },
        { header: 'ชื่อไทย',        key: 'med_thai_name',        width: 24 },
        { header: 'หมวดหมู่',       key: 'med_medical_category', width: 22 },
        { header: 'รูปแบบ',         key: 'med_dosage_form',      width: 16 },
        { header: 'หน่วย',          key: 'med_counting_unit',    width: 10 },
        { header: 'ระดับยา',        key: 'med_severity',         width: 18 },
        { header: 'ราคาต้นทุน',     key: 'med_cost_price',       width: 14 },
        { header: 'ราคาขาย',        key: 'med_selling_price',    width: 12 },
        { header: 'บัญชียาหลัก',   key: 'med_essential_med_list', width: 14 },
        { header: 'หมวดการตั้งครรภ์', key: 'med_pregnancy_category', width: 18 },
        { header: 'TMT Code',       key: 'med_TMT_code',         width: 16 },
        { header: 'วันผลิต',        key: 'mfg_date',             width: 12 },
        { header: 'วันหมดอายุ',     key: 'exp_date',             width: 14 },
      ]);
      excelRows(ws, rows, ['med_id','med_name','med_generic_name','med_marketing_name','med_thai_name',
        'med_medical_category','med_dosage_form','med_counting_unit','med_severity',
        'med_cost_price','med_selling_price','med_essential_med_list','med_pregnancy_category',
        'med_TMT_code','mfg_date','exp_date']);
    }

    else {
      throw new AppError(`ประเภทรายงาน '${report}' ไม่รองรับ`, 400);
    }

    // ── send ──────────────────────────────────────────────────
    const filename = encodeURIComponent(`pharmsub_${report}_${Date.now()}.xlsx`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${filename}`);
    await wb.xlsx.write(res);
    res.end();
  } catch (err) { next(err); }
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORT — PDF
// ═══════════════════════════════════════════════════════════════════════════════

export async function exportPdf(req: Request, res: Response, next: NextFunction) {
  try {
    const { report, date_from, date_to, ward } = req.query as Record<string, string>;

    const dateLabel = date_from && date_to
      ? `${thDate(date_from)} – ${thDate(date_to)}`
      : thDate(new Date());

    // NOTE: pdfkit ไม่รองรับ Unicode Thai ผ่าน built-in fonts
    // ต้องใช้ฟอนต์ external ที่รองรับ Thai เช่น Sarabun/Noto Sans Thai
    // ไฟล์นี้ generate PDF แบบ Landscape A4 พร้อม placeholder สำหรับ Thai font
    // ในการใช้จริง ให้ลง npm i --save @pdf-lib/fontkit แล้ว embed TTF

    const supported = ['inventory', 'stock', 'dispense', 'med-usage-history'];
    if (!supported.includes(report)) throw new AppError(`report type '${report}' ไม่รองรับ PDF export`, 400);

    const doc = new PDFDocument({ size: 'A4', layout: 'portrait', margin: 40 });

    // Register Sarabun Thai font
    if (fs.existsSync(FONT_REG))  doc.registerFont('Sarabun',      FONT_REG);
    if (fs.existsSync(FONT_BOLD)) doc.registerFont('Sarabun-Bold', FONT_BOLD);
    doc.font(fs.existsSync(FONT_REG) ? 'Sarabun' : 'Helvetica');

    let pageNum = 0;
    doc.on('pageAdded', () => {
      pageNum++;
      const W = doc.page.width; const H = doc.page.height;
      doc.save().font(fs.existsSync(FONT_REG) ? 'Sarabun' : 'Helvetica')
        .fontSize(8).fillColor('#94A3B8')
        .text(`หน้า ${pageNum}`, W - 80, H - 30, { width: 60, align: 'right' })
        .restore();
    });

    const filename = encodeURIComponent(`pharmsub_${report}_${Date.now()}.pdf`);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${filename}`);
    doc.pipe(res);

    // ── inventory ─────────────────────────────────────────────
    if (report === 'inventory') {
      const { rows } = await query(
        `SELECT COALESCE(ms.med_showname, mt.med_name) AS drug_name,
                mt.med_generic_name, mt.med_medical_category AS category,
                mt.med_counting_unit AS unit, ms.med_quantity AS current_stock,
                ms.min_quantity, ms.exp_date,
                (ms.med_quantity * COALESCE(ms.unit_price, mt.med_selling_price, 0)) AS total_value,
                CASE WHEN ms.is_expired OR ms.exp_date < NOW() THEN 'Expired'
                     WHEN ms.med_quantity = 0 THEN 'Out'
                     WHEN ms.min_quantity IS NOT NULL AND ms.med_quantity < ms.min_quantity THEN 'Low'
                     ELSE 'OK' END AS status
         FROM   ${SCHEMA}.med_subwarehouse ms
         JOIN   ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         ORDER  BY drug_name`
      );

      pdfHeader(doc, 'รายงานสินค้าคงคลัง', 'Inventory Report');

      // table header
      const cols = [160, 70, 45, 55, 50, 75, 60];
      const heads = ['ชื่อยา', 'หมวดหมู่', 'หน่วย', 'คงเหลือ', 'ขั้นต่ำ', 'วันหมดอายุ', 'สถานะ'];
      let x = 40;
      doc.font('Sarabun-Bold').fontSize(9).fillColor('#006FC6');
      heads.forEach((h, i) => { doc.text(h, x, doc.y, { width: cols[i], continued: i < heads.length - 1 }); x += cols[i]; });
      doc.fillColor('black').moveDown(0.3);
      doc.moveTo(40, doc.y).lineTo(555, doc.y).strokeColor('#CBD5E1').stroke();
      doc.moveDown(0.2);

      rows.forEach((r, idx) => {
        if (doc.y > 760) { doc.addPage({ size: 'A4', layout: 'portrait', margin: 40 }); }
        const y = doc.y;
        if (idx % 2 === 0) {
          doc.rect(40, y - 2, 515, 14).fill('#F0F7FF').fillColor('black');
        }
        x = 40;
        const statusTh: Record<string,string> = { OK: 'ปกติ', Low: 'ต่ำ', Out: 'หมดสต็อก', Expired: 'หมดอายุ' };
        const cells = [
          String(r.drug_name).slice(0, 40),
          String(r.category || '-').slice(0, 18),
          String(r.unit || '-'),
          String(r.current_stock),
          String(r.min_quantity ?? '-'),
          r.exp_date ? thDate(r.exp_date) : '-',
          statusTh[r.status] ?? r.status,
        ];
        doc.font('Sarabun').fontSize(8).fillColor(r.status !== 'OK' ? '#DC2626' : '#0F172A');
        cells.forEach((c, i) => {
          doc.text(c, x, y, { width: cols[i], lineBreak: false });
          x += cols[i];
        });
        doc.fillColor('black').moveDown(0.1);
      });

      doc.moveDown(1);
      doc.font('Sarabun').fontSize(9).text(`รวม: ${rows.length} รายการ  |  มูลค่ารวม: ${rows.reduce((s, r) => s + Number(r.total_value || 0), 0).toLocaleString('th-TH', { minimumFractionDigits: 2 })} บาท`, { align: 'right' });
    }

    // ── stock ─────────────────────────────────────────────────
    else if (report === 'stock') {
      const params: any[] = [date_from, date_to];
      const { rows } = await query(
        `SELECT TO_CHAR(st.created_at,'DD/MM/YYYY HH24:MI') AS tx_time,
                st.tx_type,
                COALESCE(ms.med_showname, mt.med_name) AS drug_name,
                mt.med_counting_unit AS unit, st.quantity,
                st.balance_before, st.balance_after,
                st.reference_no,
                COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS performed_by
         FROM   ${SCHEMA}.stock_transactions st
         JOIN   ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
         JOIN   ${SCHEMA}.med_table        mt ON mt.med_id  = st.med_id
         LEFT JOIN public.profiles         pu ON pu.id = st.performed_by
         LEFT JOIN auth.users              au ON au.id = st.performed_by
         WHERE  st.created_at >= $1 AND st.created_at < $2::date + 1
         ORDER  BY st.created_at DESC LIMIT 300`, params
      );

      pdfHeader(doc, 'รายงานความเคลื่อนไหวสต็อก', `ช่วง: ${dateLabel}`);
      const cols = [85, 55, 140, 40, 45, 50, 50, 50];
      const heads = ['วันเวลา', 'ประเภท', 'ชื่อยา', 'หน่วย', 'จำนวน', 'ก่อน', 'หลัง', 'ผู้บันทึก'];
      const txLabelTh: Record<string, string> = { in: 'รับเข้า', out: 'จ่ายออก', adjust: 'ปรับ', return: 'คืนยา', expired: 'หมดอายุ' };
      let x = 40;
      doc.font('Sarabun-Bold').fontSize(9).fillColor('#006FC6');
      heads.forEach((h, i) => { doc.text(h, x, doc.y, { width: cols[i], continued: i < heads.length - 1 }); x += cols[i]; });
      doc.fillColor('black').moveDown(0.3);
      doc.moveTo(40, doc.y).lineTo(555, doc.y).strokeColor('#CBD5E1').stroke().moveDown(0.2);

      rows.forEach((r, idx) => {
        if (doc.y > 760) doc.addPage({ size: 'A4', layout: 'portrait', margin: 40 });
        const y = doc.y;
        if (idx % 2 === 0) doc.rect(40, y - 2, 515, 14).fill('#F0F7FF').fillColor('black');
        x = 40;
        [r.tx_time, txLabelTh[r.tx_type] ?? r.tx_type, String(r.drug_name).slice(0, 32), r.unit,
          String(r.quantity), String(r.balance_before), String(r.balance_after), r.performed_by ?? '-'
        ].forEach((c, i) => { doc.font('Sarabun').fontSize(8).text(String(c), x, y, { width: cols[i], lineBreak: false }); x += cols[i]; });
        doc.moveDown(0.1);
      });
    }

    // ── dispense ──────────────────────────────────────────────
    else if (report === 'dispense') {
      const params: any[] = [date_from, date_to];
      let wf = ''; if (ward) { wf = ` AND pr.ward = $3`; params.push(ward); }
      const { rows } = await query(
        `SELECT pr.prescription_no,
                TO_CHAR(pr.created_at,'DD/MM/YYYY') AS created_date,
                CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
                pr.ward, pr.status,
                COUNT(pi.item_id)::INTEGER AS item_count
         FROM   ${SCHEMA}.prescriptions pr
         LEFT JOIN ${SCHEMA}.patient    pa ON pa.patient_id  = pr.patient_id
         LEFT JOIN ${SCHEMA}.prescription_items pi ON pi.prescription_id = pr.prescription_id
         WHERE  pr.created_at >= $1 AND pr.created_at < $2::date + 1 ${wf}
         GROUP  BY pr.prescription_id, pa.first_name, pa.last_name, pa.hn_number
         ORDER  BY pr.created_at DESC LIMIT 200`, params
      );

      pdfHeader(doc, 'รายงานการจ่ายยา', `ช่วง: ${dateLabel}${ward ? ' | วอร์ด: ' + ward : ''}`);
      const statusTh2: Record<string, string> = { pending: 'รอจ่าย', dispensed: 'จ่ายแล้ว', returned: 'คืนยา', cancelled: 'ยกเลิก' };
      const cols = [100, 65, 130, 70, 60, 55, 35];
      const heads = ['เลขใบสั่งยา', 'วันที่', 'ผู้ป่วย', 'HN', 'วอร์ด', 'สถานะ', 'รายการ'];
      let x = 40;
      doc.font('Sarabun-Bold').fontSize(9).fillColor('#006FC6');
      heads.forEach((h, i) => { doc.text(h, x, doc.y, { width: cols[i], continued: i < heads.length - 1 }); x += cols[i]; });
      doc.fillColor('black').moveDown(0.3);
      doc.moveTo(40, doc.y).lineTo(555, doc.y).strokeColor('#CBD5E1').stroke().moveDown(0.2);

      rows.forEach((r, idx) => {
        if (doc.y > 760) doc.addPage({ size: 'A4', layout: 'portrait', margin: 40 });
        const y = doc.y;
        if (idx % 2 === 0) doc.rect(40, y - 2, 515, 14).fill('#F0F7FF').fillColor('black');
        x = 40;
        [r.prescription_no, r.created_date, String(r.patient_name || 'N/A').slice(0, 24),
          r.hn_number ?? '-', r.ward ?? '-', statusTh2[r.status] ?? r.status, String(r.item_count)
        ].forEach((c, i) => { doc.font('Sarabun').fontSize(8).text(String(c), x, y, { width: cols[i], lineBreak: false }); x += cols[i]; });
        doc.moveDown(0.1);
      });
      doc.moveDown(1).font('Sarabun').fontSize(9).text(`รวม: ${rows.length} ใบสั่งยา`, { align: 'right' });
    }

    // ── med-usage-history ─────────────────────────────────────
    else if (report === 'med-usage-history') {
      const params: any[] = [];
      let w = 'WHERE 1=1'; let p = 1;
      if (date_from) { w += ` AND pr.created_at >= $${p}`; params.push(date_from); p++; }
      if (date_to)   { w += ` AND pr.created_at < $${p}::date + 1`; params.push(date_to); p++; }
      const { rows } = await query(
        `SELECT
           TO_CHAR(pr.created_at,'DD/MM/YYYY') AS created_date,
           CONCAT(pa.first_name,' ',pa.last_name) AS patient_name, pa.hn_number,
           mt.med_name, mt.med_generic_name,
           pi.quantity, mt.med_counting_unit AS unit,
           pi.dose, pi.frequency, pi.route,
           COALESCE(pu.firstname_th || ' ' || pu.lastname_th, au.email, '') AS doctor_name,
           pr.status
         FROM ${SCHEMA}.prescription_items pi
         JOIN ${SCHEMA}.prescriptions pr ON pr.prescription_id = pi.prescription_id
         LEFT JOIN ${SCHEMA}.patient pa  ON pa.patient_id = pr.patient_id
         LEFT JOIN public.profiles pu    ON pu.id = pr.doctor_id
         LEFT JOIN auth.users     au     ON au.id = pr.doctor_id
         LEFT JOIN ${SCHEMA}.med_table mt ON mt.med_id = pi.med_id
         ${w}
         ORDER BY pr.created_at DESC, pa.last_name
         LIMIT 500`, params
      );

      pdfHeader(doc, 'รายงานประวัติการใช้ยาผู้ป่วย', `ช่วง: ${dateLabel}`);
      const rxStatusTh: Record<string, string> = { dispensed: 'จ่ายแล้ว', pending: 'รอจ่าย', returned: 'คืนยา', cancelled: 'ยกเลิก' };
      const cols = [70, 65, 130, 50, 100, 60, 40];
      const heads = ['วันที่', 'HN', 'ชื่อยา', 'จำนวน', 'ขนาด/ความถี่/เส้นทาง', 'แพทย์', 'สถานะ'];
      let x = 40;
      doc.font('Sarabun-Bold').fontSize(9).fillColor('#006FC6');
      heads.forEach((h, i) => { doc.text(h, x, doc.y, { width: cols[i], continued: i < heads.length - 1 }); x += cols[i]; });
      doc.fillColor('black').moveDown(0.3);
      doc.moveTo(40, doc.y).lineTo(555, doc.y).strokeColor('#CBD5E1').stroke().moveDown(0.2);

      rows.forEach((r, idx) => {
        if (doc.y > 760) doc.addPage({ size: 'A4', layout: 'portrait', margin: 40 });
        const y = doc.y;
        if (idx % 2 === 0) doc.rect(40, y - 2, 515, 14).fill('#F0F7FF').fillColor('black');
        x = 40;
        const usage = [r.dose, r.frequency, r.route].filter(Boolean).join(' / ') || '-';
        [
          r.created_date ?? '-',
          r.hn_number ?? '-',
          String(r.med_name ?? '-').slice(0, 26),
          `${r.quantity} ${r.unit ?? ''}`.trim(),
          String(usage).slice(0, 20),
          String(r.doctor_name ?? '-').slice(0, 16),
          rxStatusTh[r.status] ?? r.status ?? '-',
        ].forEach((c, i) => { doc.font('Sarabun').fontSize(8).text(String(c), x, y, { width: cols[i], lineBreak: false }); x += cols[i]; });
        doc.moveDown(0.1);
      });
      doc.moveDown(1).font('Sarabun').fontSize(9).text(`รวม: ${rows.length} รายการ`, { align: 'right' });
    }

    doc.end();
  } catch (err) { next(err); }
}
