import { Request, Response, NextFunction } from 'express';
import { query, SCHEMA } from '../db/pool';

// ── GET /dashboard/stats ──────────────────────────────────────────────────────
export async function getDashboardStats(req: Request, res: Response, next: NextFunction) {
  try {
    const today = new Date().toISOString().slice(0, 10);

    const [
      settingRes, total, lowStock, expired,
      todayDispense, todayStockIn, todayTx,
      pendingRx, queueStats,
    ] = await Promise.all([
      query(`SELECT value FROM ${SCHEMA}.system_settings WHERE key = 'near_expiry_days'`),
      query<{ cnt: string }>(`SELECT COUNT(*) AS cnt FROM ${SCHEMA}.med_subwarehouse`),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.med_subwarehouse
        WHERE med_quantity > 0 AND min_quantity IS NOT NULL
          AND med_quantity < min_quantity AND is_expired = false`),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.med_subwarehouse
        WHERE (is_expired = true OR exp_date < NOW()) AND med_quantity > 0`),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.prescriptions
        WHERE status = 'dispensed' AND DATE(dispensed_at) = $1`, [today]),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.stock_transactions
        WHERE tx_type = 'in' AND DATE(created_at) = $1`, [today]),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.stock_transactions
        WHERE DATE(created_at) = $1`, [today]),
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.prescriptions
        WHERE status = 'pending' AND COALESCE(source, 'counter') = 'counter'`),
      query(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'waiting')   AS waiting,
          COUNT(*) FILTER (WHERE status = 'called')    AS called,
          COUNT(*) FILTER (WHERE status = 'completed') AS completed_today
        FROM ${SCHEMA}.queue_entries
        WHERE DATE(created_at) = $1`, [today]),
    ]);

    const nearExpiryDays = parseInt(settingRes.rows[0]?.value ?? '30');

    const [nearExpiry, nearExpiryTop, recentTx] = await Promise.all([
      query<{ cnt: string }>(`
        SELECT COUNT(*) AS cnt FROM ${SCHEMA}.med_subwarehouse
        WHERE exp_date BETWEEN NOW() AND NOW() + ($1 || ' days')::INTERVAL
          AND is_expired = false AND med_quantity > 0`, [nearExpiryDays]),
      query(`
        SELECT ms.med_sid,
          COALESCE(ms.med_showname, mt.med_name) AS drug_name,
          ms.exp_date, ms.med_quantity,
          (ms.exp_date::date - CURRENT_DATE) AS days_left
        FROM ${SCHEMA}.med_subwarehouse ms
        JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
        WHERE ms.exp_date BETWEEN NOW() AND NOW() + ($1 || ' days')::INTERVAL
          AND ms.is_expired = false AND ms.med_quantity > 0
        ORDER BY ms.exp_date ASC LIMIT 6`, [nearExpiryDays]),
      query(`
        SELECT st.tx_id, st.tx_type, st.quantity, st.created_at,
          COALESCE(ms.med_showname, mt.med_name) AS drug_name
        FROM ${SCHEMA}.stock_transactions st
        JOIN ${SCHEMA}.med_subwarehouse ms ON ms.med_sid = st.med_sid
        JOIN ${SCHEMA}.med_table mt ON mt.med_id = st.med_id
        ORDER BY st.created_at DESC LIMIT 8`),
    ]);

    const qs = queueStats.rows[0] ?? {};

    res.json({
      total_drugs:              parseInt(total.rows[0]?.cnt ?? '0'),
      low_stock_count:          parseInt(lowStock.rows[0]?.cnt ?? '0'),
      near_expiry_count:        parseInt(nearExpiry.rows[0]?.cnt ?? '0'),
      expired_count:            parseInt(expired.rows[0]?.cnt ?? '0'),
      today_dispense_count:     parseInt(todayDispense.rows[0]?.cnt ?? '0'),
      today_stock_in_count:     parseInt(todayStockIn.rows[0]?.cnt ?? '0'),
      total_transactions_today: parseInt(todayTx.rows[0]?.cnt ?? '0'),
      pending_prescriptions:    parseInt(pendingRx.rows[0]?.cnt ?? '0'),
      queue_waiting:            parseInt(qs.waiting ?? '0'),
      queue_called:             parseInt(qs.called ?? '0'),
      queue_completed_today:    parseInt(qs.completed_today ?? '0'),
      near_expiry_top:          nearExpiryTop.rows,
      recent_transactions:      recentTx.rows,
    });
  } catch (err) { next(err); }
}

// ── GET /dashboard/stock-summary ──────────────────────────────────────────────
export async function getStockSummary(req: Request, res: Response, next: NextFunction) {
  try {
    const days = parseInt(req.query.days as string) || 30;
    const { rows } = await query(
      `SELECT
         TO_CHAR(d.date, 'YYYY-MM-DD') AS date,
         COALESCE(SUM(CASE WHEN st.tx_type = 'in'     THEN st.quantity      ELSE 0 END), 0) AS stock_in,
         COALESCE(SUM(CASE WHEN st.tx_type = 'out'    THEN ABS(st.quantity) ELSE 0 END), 0) AS stock_out,
         COALESCE(SUM(CASE WHEN st.tx_type = 'return' THEN st.quantity      ELSE 0 END), 0) AS stock_return,
         COALESCE(rx.cnt, 0) AS dispensed_count
       FROM generate_series(
         CURRENT_DATE - ($1 - 1) * INTERVAL '1 day',
         CURRENT_DATE,
         '1 day'::interval
       ) AS d(date)
       LEFT JOIN ${SCHEMA}.stock_transactions st ON DATE(st.created_at) = d.date
       LEFT JOIN (
         SELECT DATE(dispensed_at) AS date, COUNT(*) AS cnt
         FROM ${SCHEMA}.prescriptions
         WHERE status = 'dispensed'
           AND dispensed_at >= CURRENT_DATE - ($1 - 1) * INTERVAL '1 day'
         GROUP BY DATE(dispensed_at)
       ) rx ON rx.date = d.date
       GROUP BY d.date, rx.cnt
       ORDER BY d.date ASC`,
      [days]
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ── GET /alerts ───────────────────────────────────────────────────────────────
export async function getAlerts(req: Request, res: Response, next: NextFunction) {
  try {
    const { is_read, type } = req.query;

    const { rows: settingRows } = await query(
      `SELECT key, value FROM ${SCHEMA}.system_settings
       WHERE key IN ('near_expiry_days','alert_enabled_low_stock','alert_enabled_near_expiry',
                     'alert_enabled_expired','alert_enabled_overstock','alert_enabled_new_drug')`
    );
    const settingMap: Record<string, string> = {};
    for (const r of settingRows) settingMap[r.key] = r.value;

    const nearExpiryDays = parseInt(settingMap['near_expiry_days'] ?? '30');
    const enabled = (key: string, def = true) =>
      settingMap[key] !== undefined ? settingMap[key] === 'true' : def;

    const liveAlerts: any[] = [];

    // Low stock
    if (enabled('alert_enabled_low_stock')) {
      const { rows: lowRows } = await query(
        `SELECT
           ms.med_sid, ms.med_quantity, ms.min_quantity,
           COALESCE(ms.med_showname, mt.med_name) AS drug_name
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         WHERE ms.med_quantity > 0
           AND ms.min_quantity IS NOT NULL
           AND ms.med_quantity < ms.min_quantity
           AND ms.is_expired = false
         ORDER BY (ms.med_quantity::float / NULLIF(ms.min_quantity,0)) ASC
         LIMIT 50`
      );
      for (const r of lowRows) {
        liveAlerts.push({
          alert_type: 'low_stock',
          med_sid: r.med_sid,
          drug_name: r.drug_name,
          message: `สต็อกเหลือ ${r.med_quantity} หน่วย (ขั้นต่ำ ${r.min_quantity})`,
          severity: r.med_quantity < r.min_quantity * 0.5 ? 'critical' : 'warning',
        });
      }
    }

    // Near expiry
    if (enabled('alert_enabled_near_expiry')) {
      const { rows: nearRows } = await query(
        `SELECT
           ms.med_sid, ms.exp_date,
           COALESCE(ms.med_showname, mt.med_name) AS drug_name,
           (ms.exp_date::date - CURRENT_DATE) AS days_left
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         WHERE ms.exp_date BETWEEN NOW() AND NOW() + ($1 || ' days')::INTERVAL
           AND ms.is_expired = false AND ms.med_quantity > 0
         ORDER BY ms.exp_date ASC
         LIMIT 50`,
        [nearExpiryDays]
      );
      for (const r of nearRows) {
        liveAlerts.push({
          alert_type: 'near_expiry',
          med_sid: r.med_sid,
          drug_name: r.drug_name,
          message: `ยาจะหมดอายุใน ${r.days_left} วัน (${new Date(r.exp_date).toLocaleDateString('th-TH')})`,
          severity: r.days_left <= 7 ? 'critical' : 'warning',
        });
      }
    }

    // Expired
    if (enabled('alert_enabled_expired')) {
      const { rows: expRows } = await query(
        `SELECT
           ms.med_sid, ms.exp_date, ms.med_quantity,
           COALESCE(ms.med_showname, mt.med_name) AS drug_name
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         WHERE (ms.is_expired = true OR ms.exp_date < NOW())
           AND ms.med_quantity > 0
         ORDER BY ms.exp_date ASC
         LIMIT 50`
      );
      for (const r of expRows) {
        liveAlerts.push({
          alert_type: 'expired',
          med_sid: r.med_sid,
          drug_name: r.drug_name,
          message: `ยาหมดอายุแล้ว คงเหลือ ${r.med_quantity} หน่วย`,
          severity: 'critical',
        });
      }
    }

    // New drug (added within last 7 days)
    if (enabled('alert_enabled_new_drug')) {
      const { rows: newRows } = await query(
        `SELECT ms.med_sid, COALESCE(ms.med_showname, mt.med_name) AS drug_name, ms.created_at
         FROM ${SCHEMA}.med_subwarehouse ms
         JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
         WHERE ms.created_at >= NOW() - INTERVAL '7 days'
         ORDER BY ms.created_at DESC
         LIMIT 30`
      );
      for (const r of newRows) {
        liveAlerts.push({
          alert_type: 'new_drug',
          med_sid: r.med_sid,
          drug_name: r.drug_name,
          message: `ยาใหม่เพิ่มเข้าคลังเมื่อ ${new Date(r.created_at).toLocaleDateString('th-TH')}`,
          severity: 'warning',
        });
      }
    }

    // Incomplete drug records
    const { rows: incompleteRows } = await query(
      `SELECT ms.med_sid, COALESCE(ms.med_showname, mt.med_name) AS drug_name
       FROM ${SCHEMA}.med_subwarehouse ms
       JOIN ${SCHEMA}.med_table mt ON mt.med_id = ms.med_id
       WHERE ms.med_showname IS NULL
          OR ms.cost_price   IS NULL
          OR ms.unit_price   IS NULL
          OR ms.min_quantity IS NULL
       ORDER BY ms.created_at DESC
       LIMIT 50`
    );
    for (const r of incompleteRows) {
      liveAlerts.push({
        alert_type: 'incomplete_record',
        med_sid: r.med_sid,
        drug_name: r.drug_name,
        message: 'ข้อมูลยายังไม่ครบถ้วน กรุณาเพิ่มชื่อแสดง, ราคา และสต็อกขั้นต่ำ',
        severity: 'warning',
      });
    }

    // Upcoming cut-off
    try {
      const { rows: cutRows } = await query(
        `SELECT mcp.*, sw.name AS warehouse_name
         FROM ${SCHEMA}.med_cut_off_period mcp
         JOIN ${SCHEMA}.sub_warehouse sw ON sw.sub_warehouse_id = mcp.sub_warehouse_id
         WHERE mcp.is_active = true
           AND (mcp.last_executed_at IS NULL OR mcp.last_executed_at::date < CURRENT_DATE)
           AND (
             (EXTRACT(DAY FROM NOW()) <= mcp.period_day AND mcp.period_day - EXTRACT(DAY FROM NOW()) <= 3)
           )`
      );
      for (const r of cutRows) {
        liveAlerts.push({
          alert_type: 'cut_off',
          med_sid: null,
          drug_name: r.warehouse_name,
          message: `ใกล้ถึงรอบตัดยา "${r.warehouse_name}" วันที่ ${r.period_day} เวลา ${String(r.period_time_h??0).padStart(2,'0')}:${String(r.period_time_m??0).padStart(2,'0')}`,
          severity: 'warning',
          cut_off_id: r.cut_off_period_id ?? r.id,
        });
      }
    } catch { /* cut_off_period may not have last_executed_at yet */ }

    const NON_ENUM_TYPES = new Set(['new_drug']);
    const persistableAlerts = liveAlerts.filter(a => a.med_sid != null && !NON_ENUM_TYPES.has(a.alert_type));
    if (persistableAlerts.length > 0) {
      const values = persistableAlerts
        .map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
        .join(', ');
      const params = persistableAlerts.flatMap(a => [a.med_sid, a.alert_type, false]);
      await query(
        `INSERT INTO ${SCHEMA}.alert_log (med_sid, alert_type, is_read)
         VALUES ${values}
         ON CONFLICT (med_sid, alert_type) DO NOTHING`,
        params
      );
    }

    const { rows: logRows } = await query(
      `SELECT med_sid, alert_type, is_read, created_at FROM ${SCHEMA}.alert_log`
    );
    const logMap = new Map(logRows.map(r => [
      `${r.med_sid}_${r.alert_type}`,
      { is_read: r.is_read, created_at: r.created_at },
    ]));

    let result = liveAlerts.map((a) => {
      const log = a.med_sid ? logMap.get(`${a.med_sid}_${a.alert_type}`) : undefined;
      return {
        id: a.med_sid ? `${a.med_sid}_${a.alert_type}` : `cutoff_${a.cut_off_id ?? Math.random()}`,
        ...a,
        is_read:    log?.is_read ?? false,
        created_at: log?.created_at ?? new Date(),
      };
    });

    if (type)    result = result.filter(a => a.alert_type === type);
    if (is_read === 'true')  result = result.filter(a => a.is_read);
    if (is_read === 'false') result = result.filter(a => !a.is_read);

    res.json(result);
  } catch (err) { next(err); }
}

// ── PUT /alerts/:med_sid/read ─────────────────────────────────────────────────
export async function markAlertRead(req: Request, res: Response, next: NextFunction) {
  try {
    const { med_sid } = req.params;
    const { alert_type } = req.body;
    await query(
      `INSERT INTO ${SCHEMA}.alert_log (med_sid, alert_type, is_read)
       VALUES ($1, $2, true)
       ON CONFLICT (med_sid, alert_type) DO UPDATE SET is_read = true`,
      [med_sid, alert_type || 'low_stock']
    );
    res.json({ success: true });
  } catch (err) { next(err); }
}

// ── PUT /alerts/read-all ──────────────────────────────────────────────────────
export async function markAllAlertsRead(req: Request, res: Response, next: NextFunction) {
  try {
    await query(`UPDATE ${SCHEMA}.alert_log SET is_read = true WHERE is_read = false`);
    res.json({ success: true });
  } catch (err) { next(err); }
}

// ── GET /settings ─────────────────────────────────────────────────────────────
export async function getSettings(req: Request, res: Response, next: NextFunction) {
  try {
    const { rows } = await query(`SELECT key, value FROM ${SCHEMA}.system_settings`);
    const settings: Record<string, string> = {};
    for (const r of rows) settings[r.key] = r.value;
    res.json(settings);
  } catch (err) { next(err); }
}

// ── PUT /settings ─────────────────────────────────────────────────────────────
export async function updateSettings(req: Request, res: Response, next: NextFunction) {
  try {
    const entries = Object.entries(req.body as Record<string, string>);
    for (const [key, value] of entries) {
      await query(
        `INSERT INTO ${SCHEMA}.system_settings (key, value, updated_at)
         VALUES ($1,$2,NOW())
         ON CONFLICT (key) DO UPDATE SET value=$2, updated_at=NOW()`,
        [key, String(value)]
      );
    }
    res.json({ message: 'บันทึกแล้ว' });
  } catch (err) { next(err); }
}
