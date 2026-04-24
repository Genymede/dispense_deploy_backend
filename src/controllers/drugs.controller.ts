import { Request, Response, NextFunction } from 'express';
import { query, SCHEMA } from '../db/pool';
import { AppError } from '../middleware/errorHandler';

// ── GET /drugs — รายการยาใน subwarehouse พร้อม med_table ──────────────────────
export async function getDrugs(req: Request, res: Response, next: NextFunction) {
  try {
    const { search, category, status, low_stock, near_expiry, expired, limit = 200, offset = 0 } = req.query;

    let where = `WHERE 1=1`;
    const params: any[] = [];
    let p = 1;

    if (search) {
      where += ` AND (
        ms.med_showname ILIKE $${p} OR
        ms.med_showname_eng ILIKE $${p} OR
        mt.med_name ILIKE $${p} OR
        mt.med_generic_name ILIKE $${p} OR
        mt.med_marketing_name ILIKE $${p} OR
        mt.med_thai_name ILIKE $${p}
      )`;
      params.push(`%${search}%`); p++;
    }
    if (category) {
      where += ` AND mt.med_medical_category = $${p}`;
      params.push(category); p++;
    }
    if (status === 'out_of_stock') {
      where += ` AND ms.med_quantity = 0`;
    }
    if (low_stock === '1') {
      where += ` AND ms.med_quantity > 0 AND ms.min_quantity IS NOT NULL AND ms.med_quantity < ms.min_quantity`;
    }
    if (near_expiry === '1') {
      where += ` AND ms.exp_date IS NOT NULL AND ms.exp_date BETWEEN NOW() AND NOW() + INTERVAL '30 days' AND ms.is_expired = false`;
    }
    if (expired === '1') {
      where += ` AND (ms.is_expired = true OR ms.exp_date < NOW())`;
    }

    const sql = `
      SELECT
        ms.med_sid,
        ms.med_id,
        ms.med_quantity        AS current_stock,
        ms.packaging_type,
        ms.is_divisible,
        ms.location,
        ms.med_showname,
        ms.med_showname_eng,
        ms.min_quantity,
        ms.max_quantity,
        ms.cost_price,
        ms.unit_price,
        ms.mfg_date,
        ms.exp_date,
        ms.is_expired,
        ms.created_at,
        ms.updated_at,
        mt.med_name,
        mt.med_generic_name,
        mt.med_marketing_name,
        mt.med_thai_name,
        mt.med_medical_category AS category,
        mt.med_dosage_form,
        mt.med_severity,
        mt.med_counting_unit    AS unit,
        mt.med_out_of_stock,
        mt.med_cost_price       AS list_cost_price,
        mt.med_selling_price    AS list_selling_price,
        mt.med_exp              AS registered_exp,
        mt.med_mfg              AS registered_mfg,
        COALESCE(lc.lot_count, 0) AS lot_count,
        lc.nearest_lot_exp
      FROM ${SCHEMA}.med_subwarehouse ms
      JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
      LEFT JOIN (
        SELECT med_sid, COUNT(*)::int AS lot_count, MIN(exp_date) AS nearest_lot_exp
        FROM ${SCHEMA}.med_stock_lots
        GROUP BY med_sid
      ) lc ON lc.med_sid = ms.med_sid
      ${where}
      ORDER BY lc.nearest_lot_exp ASC NULLS LAST, ms.med_showname NULLS LAST, mt.med_name
      LIMIT $${p} OFFSET $${p + 1}
    `;
    params.push(Number(limit), Number(offset));

    const { rows } = await query(sql, params);

    // count
    const countSql = `
      SELECT COUNT(*) AS total
      FROM ${SCHEMA}.med_subwarehouse ms
      JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
      ${where}
    `;
    const countParams = params.slice(0, -2);
    const countRes = await query(countSql, countParams);

    res.json({ data: rows, total: parseInt(countRes.rows[0]?.total ?? '0') });
  } catch (err) { next(err); }
}

// ── GET /drugs/categories ─────────────────────────────────────────────────────
export async function getCategories(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query<{ category: string }>(
      `SELECT DISTINCT med_medical_category AS category
       FROM ${SCHEMA}.med_table
       WHERE med_medical_category IS NOT NULL
       ORDER BY category`
    );
    res.json(rows.map(r => r.category));
  } catch (err) { next(err); }
}

// ── GET /drugs/:med_sid ───────────────────────────────────────────────────────
export async function getDrugById(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_sid } = req.params;
    const { rows } = await query(
      `SELECT
        ms.*, mt.med_name, mt.med_generic_name, mt.med_marketing_name,
        mt.med_thai_name, mt.med_medical_category AS category,
        mt.med_dosage_form, mt.med_severity, mt.med_counting_unit AS unit,
        mt.med_selling_price AS list_selling_price,
        mt.med_pregnancy_category
       FROM ${SCHEMA}.med_subwarehouse ms
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       WHERE ms.med_sid = $1`,
      [med_sid]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการยา', 404);
    const { rows: lots } = await query(
      `SELECT lot_id, lot_number, quantity, exp_date, mfg_date, received_at, note
       FROM ${SCHEMA}.med_stock_lots WHERE med_sid = $1
       ORDER BY exp_date ASC NULLS LAST, lot_id ASC`,
      [med_sid]
    );
    res.json({ ...rows[0], lots });
  } catch (err) { next(err); }
}

// ── GET /drugs/:med_sid/lots ──────────────────────────────────────────────────
export async function getLots(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_sid } = req.params;
    const { rows } = await query(
      `SELECT lot_id, lot_number, quantity, exp_date, mfg_date, received_at, note
       FROM ${SCHEMA}.med_stock_lots WHERE med_sid = $1
       ORDER BY exp_date ASC NULLS LAST, lot_id ASC`,
      [med_sid]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ── POST /drugs — เพิ่มยาใน subwarehouse ─────────────────────────────────────
export async function createDrug(req: Request, res: Response, next: NextFunction) {
  try {
    const {
      med_id, med_quantity = 0, packaging_type, is_divisible = false,
      location, med_showname, med_showname_eng,
      min_quantity, max_quantity, cost_price, unit_price,
      mfg_date, exp_date
    } = req.body;

    if (!med_id || !packaging_type) throw new AppError('med_id และ packaging_type จำเป็น', 400);

    // verify med_id exists
    const check = await query(`SELECT med_id FROM ${SCHEMA}.med_table WHERE med_id = $1`, [med_id]);
    if (!check.rows.length) throw new AppError('ไม่พบ med_id ในตาราง med_table', 404);

    const { rows } = await query(
      `INSERT INTO ${SCHEMA}.med_subwarehouse
         (med_id, med_quantity, packaging_type, is_divisible, location,
          med_showname, med_showname_eng, min_quantity, max_quantity,
          cost_price, unit_price, mfg_date, exp_date)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       RETURNING *`,
      [med_id, med_quantity, packaging_type, is_divisible, location,
       med_showname, med_showname_eng, min_quantity, max_quantity,
       cost_price, unit_price, mfg_date, exp_date]
    );
    res.status(201).json(rows[0]);
  } catch (err) { next(err); }
}

// ── PUT /drugs/:med_sid ───────────────────────────────────────────────────────
export async function updateDrug(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_sid } = req.params;
    const {
      packaging_type, is_divisible, location,
      med_showname, med_showname_eng,
      min_quantity, max_quantity, cost_price, unit_price,
      mfg_date, exp_date, is_expired
    } = req.body;

    const { rows } = await query(
      `UPDATE ${SCHEMA}.med_subwarehouse SET
        packaging_type  = COALESCE($1, packaging_type),
        is_divisible    = COALESCE($2, is_divisible),
        location        = COALESCE($3, location),
        med_showname    = COALESCE($4, med_showname),
        med_showname_eng= COALESCE($5, med_showname_eng),
        min_quantity    = COALESCE($6, min_quantity),
        max_quantity    = COALESCE($7, max_quantity),
        cost_price      = COALESCE($8, cost_price),
        unit_price      = COALESCE($9, unit_price),
        mfg_date        = COALESCE($10, mfg_date),
        exp_date        = COALESCE($11, exp_date),
        is_expired      = COALESCE($12, is_expired),
        updated_at      = NOW()
       WHERE med_sid = $13
       RETURNING *`,
      [packaging_type, is_divisible, location,
       med_showname, med_showname_eng,
       min_quantity, max_quantity, cost_price, unit_price,
       mfg_date, exp_date, is_expired, med_sid]
    );
    if (!rows.length) throw new AppError('ไม่พบรายการยา', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── DELETE /drugs/:med_sid ────────────────────────────────────────────────────
export async function deleteDrug(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_sid } = req.params;
    const { rowCount } = await query(
      `DELETE FROM ${SCHEMA}.med_subwarehouse WHERE med_sid = $1`,
      [med_sid]
    );
    if (!rowCount) throw new AppError('ไม่พบรายการยา', 404);
    res.json({ message: 'ลบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ── GET /drugs/med-table — รายการ med_table ทั้งหมด (เพื่อ dropdown เพิ่มยา) ──
export async function getMedTable(req: Request, res: Response, next: NextFunction) {
  try {
    const { search } = req.query;
    let sql = `SELECT med_id, med_name, med_generic_name, med_marketing_name,
                      med_medical_category, med_counting_unit, med_dosage_form,
                      med_cost_price, med_selling_price, med_out_of_stock
               FROM ${SCHEMA}.med_table WHERE 1=1`;
    const params: any[] = [];
    if (search) {
      sql += ` AND (med_name ILIKE $1 OR med_generic_name ILIKE $1 OR med_marketing_name ILIKE $1)`;
      params.push(`%${search}%`);
    }
    sql += ' ORDER BY med_name LIMIT 100';
    const { rows } = await query(sql, params);
    res.json(rows);
  } catch (err) { next(err); }
}
