/**
 * Migration — dispense_02 schema
 * รัน: npm run db:migrate
 */
import { pool, SCHEMA } from './pool';
import dotenv from 'dotenv';
dotenv.config();

const migrations: string[] = [
  `CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`,

  // stock_transactions
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.stock_transactions (
    tx_id           SERIAL PRIMARY KEY,
    med_sid         INTEGER NOT NULL REFERENCES ${SCHEMA}.med_subwarehouse(med_sid) ON DELETE CASCADE,
    med_id          INTEGER NOT NULL REFERENCES ${SCHEMA}.med_table(med_id),
    tx_type         VARCHAR(20) NOT NULL CHECK (tx_type IN ('in','out','adjust','return','expired')),
    quantity        INTEGER NOT NULL,
    balance_before  INTEGER NOT NULL,
    balance_after   INTEGER NOT NULL,
    lot_number      VARCHAR(100),
    expiry_date     DATE,
    reference_no    VARCHAR(100),
    prescription_no VARCHAR(50),
    ward_from       VARCHAR(100),
    performed_by    INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL,
    note            TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
  )`,

  // prescriptions
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    prescription_no VARCHAR(50) UNIQUE NOT NULL,
    patient_id      INTEGER REFERENCES ${SCHEMA}.patient(patient_id) ON DELETE SET NULL,
    doctor_id       INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL,
    ward            VARCHAR(100),
    status          VARCHAR(20) DEFAULT 'pending'
                    CHECK (status IN ('pending','dispensed','returned','cancelled')),
    dispensed_by    INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL,
    dispensed_at    TIMESTAMP,
    note            TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
  )`,

  // prescription_items
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.prescription_items (
    item_id         SERIAL PRIMARY KEY,
    prescription_id INTEGER NOT NULL REFERENCES ${SCHEMA}.prescriptions(prescription_id) ON DELETE CASCADE,
    med_sid         INTEGER NOT NULL REFERENCES ${SCHEMA}.med_subwarehouse(med_sid),
    med_id          INTEGER NOT NULL REFERENCES ${SCHEMA}.med_table(med_id),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    dose            VARCHAR(100),
    frequency       VARCHAR(50),
    route           VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
  )`,

  // alert_log
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.alert_log (
    alert_id    SERIAL PRIMARY KEY,
    med_sid     INTEGER REFERENCES ${SCHEMA}.med_subwarehouse(med_sid) ON DELETE CASCADE,
    alert_type  VARCHAR(30) NOT NULL CHECK (alert_type IN ('low_stock','near_expiry','expired','overstock')),
    severity    VARCHAR(10) DEFAULT 'warning' CHECK (severity IN ('warning','critical','info')),
    is_read     BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_alert_med_type UNIQUE (med_sid, alert_type)
  )`,

  // indexes
  `CREATE INDEX IF NOT EXISTS idx_stock_tx_med_sid  ON ${SCHEMA}.stock_transactions(med_sid)`,
  `CREATE INDEX IF NOT EXISTS idx_stock_tx_type     ON ${SCHEMA}.stock_transactions(tx_type)`,
  `CREATE INDEX IF NOT EXISTS idx_stock_tx_created  ON ${SCHEMA}.stock_transactions(created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_stock_tx_date     ON ${SCHEMA}.stock_transactions(created_at, tx_type)`,
  `ALTER TABLE ${SCHEMA}.prescriptions ADD COLUMN IF NOT EXISTS diagnosis TEXT`,
  `CREATE INDEX IF NOT EXISTS idx_rx_status         ON ${SCHEMA}.prescriptions(status)`,
  `CREATE INDEX IF NOT EXISTS idx_rx_patient        ON ${SCHEMA}.prescriptions(patient_id)`,
  `CREATE INDEX IF NOT EXISTS idx_rx_created        ON ${SCHEMA}.prescriptions(created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_rx_items_rx       ON ${SCHEMA}.prescription_items(prescription_id)`,
  `CREATE INDEX IF NOT EXISTS idx_alert_unread      ON ${SCHEMA}.alert_log(is_read, created_at DESC)`,

  // med_delivery: add medicine_list column if not exists
  `ALTER TABLE ${SCHEMA}.med_delivery ADD COLUMN IF NOT EXISTS medicine_list JSONB`,

  // med_delivery: fix doctor_id — drop bad DEFAULT (10) and NOT NULL constraint
  `ALTER TABLE ${SCHEMA}.med_delivery ALTER COLUMN doctor_id DROP DEFAULT`,
  `ALTER TABLE ${SCHEMA}.med_delivery ALTER COLUMN doctor_id DROP NOT NULL`,

  // med_delivery: NULL out existing rows where doctor_id references a non-existent auth user
  `UPDATE ${SCHEMA}.med_delivery
   SET doctor_id = NULL
   WHERE doctor_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM auth.users u WHERE u.id = med_delivery.doctor_id)`,

  // system_settings
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.system_settings (
  key       VARCHAR(100) PRIMARY KEY,
  value     TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW()
)`,
  `INSERT INTO ${SCHEMA}.system_settings (key, value)
 VALUES ('near_expiry_days','30'),('low_stock_pct','100'),('queue_current_number','0')
 ON CONFLICT (key) DO NOTHING`,

  // med_cut_off_period: add last_executed_at
  `ALTER TABLE ${SCHEMA}.med_cut_off_period ADD COLUMN IF NOT EXISTS last_executed_at TIMESTAMP`,

  // queue_entries
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.queue_entries (
    queue_id     SERIAL PRIMARY KEY,
    queue_number VARCHAR(10) NOT NULL,
    patient_id   INTEGER REFERENCES ${SCHEMA}.patient(patient_id) ON DELETE SET NULL,
    status       VARCHAR(20) DEFAULT 'waiting'
                 CHECK (status IN ('waiting','called','completed','skipped')),
    note         TEXT,
    called_by    INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL,
    called_at    TIMESTAMP,
    completed_at TIMESTAMP,
    created_at   TIMESTAMP DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_queue_status ON ${SCHEMA}.queue_entries(status)`,
  `CREATE INDEX IF NOT EXISTS idx_queue_date   ON ${SCHEMA}.queue_entries(created_at)`,

  // updated_at trigger function
  `CREATE OR REPLACE FUNCTION ${SCHEMA}.update_updated_at_column()
   RETURNS TRIGGER LANGUAGE plpgsql AS $$
   BEGIN NEW.updated_at := NOW(); RETURN NEW; END; $$`,

  // trigger on prescriptions
  `DO $$ BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_rx_updated_at') THEN
       CREATE TRIGGER trg_rx_updated_at
         BEFORE UPDATE ON ${SCHEMA}.prescriptions
         FOR EACH ROW EXECUTE FUNCTION ${SCHEMA}.update_updated_at_column();
     END IF;
   END $$`,

  // ── med_stock_lots — lot tracking per drug ────────────────────────────────
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.med_stock_lots (
    lot_id      SERIAL PRIMARY KEY,
    med_sid     INTEGER NOT NULL
                REFERENCES ${SCHEMA}.med_subwarehouse(med_sid) ON DELETE CASCADE,
    lot_number  VARCHAR(100),
    quantity    INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    exp_date    DATE,
    mfg_date    DATE,
    received_at TIMESTAMP DEFAULT NOW(),
    note        TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_lots_med_sid ON ${SCHEMA}.med_stock_lots(med_sid)`,
  `CREATE INDEX IF NOT EXISTS idx_lots_fefo    ON ${SCHEMA}.med_stock_lots(med_sid, exp_date ASC NULLS LAST)`,

  // queue: received_at + received_by สำหรับยืนยันผู้ป่วยรับยาแล้ว
  `ALTER TABLE ${SCHEMA}.queue_entries ADD COLUMN IF NOT EXISTS received_at TIMESTAMP`,
  `ALTER TABLE ${SCHEMA}.queue_entries ADD COLUMN IF NOT EXISTS received_by INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL`,

  // seed: สร้าง lot เริ่มต้นจากสต็อกเดิมที่มีอยู่แล้ว (LEGACY lot)
  `INSERT INTO ${SCHEMA}.med_stock_lots (med_sid, lot_number, quantity, exp_date, mfg_date, note)
   SELECT ms.med_sid,
          'LEGACY-' || ms.med_sid::text,
          ms.med_quantity,
          ms.exp_date,
          ms.mfg_date,
          'สร้างอัตโนมัติจากสต็อกเดิม'
   FROM   ${SCHEMA}.med_subwarehouse ms
   WHERE  ms.med_quantity > 0
     AND  NOT EXISTS (
       SELECT 1 FROM ${SCHEMA}.med_stock_lots l WHERE l.med_sid = ms.med_sid
     )`,

  // sessions: เก็บ session ใน PostgreSQL แทน in-memory Map
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.sessions (
    token       VARCHAR(64) PRIMARY KEY,
    uid         INTEGER NOT NULL REFERENCES ${SCHEMA}.users(uid) ON DELETE CASCADE,
    username    VARCHAR(100) NOT NULL,
    role_id     INTEGER NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_uid        ON ${SCHEMA}.sessions(uid)`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON ${SCHEMA}.sessions(expires_at)`,

  // stock_transactions: approval workflow
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS approval_status VARCHAR(20) DEFAULT 'approved'`,
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS approved_by INTEGER REFERENCES ${SCHEMA}.users(uid) ON DELETE SET NULL`,
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP`,
  `CREATE INDEX IF NOT EXISTS idx_stock_tx_approval ON ${SCHEMA}.stock_transactions(approval_status) WHERE approval_status = 'pending'`,

  // ── indexes สำหรับ safety check (allergy, ADR, interaction) ─────────────────
  `CREATE INDEX IF NOT EXISTS idx_allergy_patient    ON ${SCHEMA}.allergy_registry(patient_id)`,
  `CREATE INDEX IF NOT EXISTS idx_allergy_med        ON ${SCHEMA}.allergy_registry(med_id)`,
  `CREATE INDEX IF NOT EXISTS idx_adr_patient        ON ${SCHEMA}.adr_registry(patient_id)`,
  `CREATE INDEX IF NOT EXISTS idx_adr_med            ON ${SCHEMA}.adr_registry(med_id)`,
  `CREATE INDEX IF NOT EXISTS idx_interaction_med1   ON ${SCHEMA}.med_interaction(med_id_1) WHERE is_active = true`,
  `CREATE INDEX IF NOT EXISTS idx_interaction_med2   ON ${SCHEMA}.med_interaction(med_id_2) WHERE is_active = true`,
  `CREATE INDEX IF NOT EXISTS idx_queue_patient      ON ${SCHEMA}.queue_entries(patient_id)`,
  `CREATE INDEX IF NOT EXISTS idx_rx_status_created  ON ${SCHEMA}.prescriptions(status, created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_rx_items_med_sid   ON ${SCHEMA}.prescription_items(med_sid)`,

  // med_delivery: ข้อมูลผู้จัดส่งยา
  `ALTER TABLE ${SCHEMA}.med_delivery ADD COLUMN IF NOT EXISTS courier_name    TEXT`,
  `ALTER TABLE ${SCHEMA}.med_delivery ADD COLUMN IF NOT EXISTS courier_phone   TEXT`,
  `ALTER TABLE ${SCHEMA}.med_delivery ADD COLUMN IF NOT EXISTS tracking_number TEXT`,
  `ALTER TABLE ${SCHEMA}.med_delivery ADD COLUMN IF NOT EXISTS delivered_at    TIMESTAMPTZ`,

  // ── med_subwarehouse: เพิ่มคอลัมน์ให้ครบเหมือน inventory.items ──────────────
  `ALTER TABLE ${SCHEMA}.med_subwarehouse ADD COLUMN IF NOT EXISTS drug_code     VARCHAR(50)`,
  `ALTER TABLE ${SCHEMA}.med_subwarehouse ADD COLUMN IF NOT EXISTS image_url     TEXT`,
  // main_item_id = inventory.items.id (UUID) — bridge ระหว่างคลังหลักกับคลังย่อย
  `ALTER TABLE ${SCHEMA}.med_subwarehouse ADD COLUMN IF NOT EXISTS main_item_id  UUID`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_sub_main_item_id ON ${SCHEMA}.med_subwarehouse(main_item_id) WHERE main_item_id IS NOT NULL`,

  // ── stock requisition: เพิ่มคอลัมน์สำหรับรับยาจากคลังหลัก ─────────────────
  // source_type: ระบุแหล่งที่มา เช่น 'main_warehouse', 'supplier', 'adjust'
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS source_type VARCHAR(30)`,
  // mfg_date: วันผลิต — บันทึกตอน stockIn เพื่อส่งต่อไปสร้าง lot ตอน approve
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS mfg_date DATE`,
  // lot_id: FK กลับไปยัง lot ที่สร้างตอน approve (ใช้ trace transaction ↔ lot)
  `ALTER TABLE ${SCHEMA}.stock_transactions ADD COLUMN IF NOT EXISTS lot_id INTEGER REFERENCES ${SCHEMA}.med_stock_lots(lot_id) ON DELETE SET NULL`,
  // med_stock_lots: status สำหรับ quarantine / recalled / expired tracking
  `ALTER TABLE ${SCHEMA}.med_stock_lots ADD COLUMN IF NOT EXISTS status     VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active','quarantine','recalled','expired'))`,
  `ALTER TABLE ${SCHEMA}.med_stock_lots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP  DEFAULT NOW()`,

  // alert_log: ขยาย ENUM alert_type_enum ให้รองรับ incomplete_record
  `ALTER TYPE ${SCHEMA}.alert_type_enum ADD VALUE IF NOT EXISTS 'incomplete_record'`,

  // units_per_pack ถูกยกเลิก — ระบบจัดการสต็อกเป็นเม็ดอย่างเดียว
  `ALTER TABLE ${SCHEMA}.med_subwarehouse DROP COLUMN IF EXISTS units_per_pack`,

  // ── med_table: ย้าย main_item_id มาไว้ที่ทะเบียนยาหลัก ───────────────────────
  `ALTER TABLE ${SCHEMA}.med_table ADD COLUMN IF NOT EXISTS main_item_id UUID`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_med_table_main_item_id
   ON ${SCHEMA}.med_table(main_item_id) WHERE main_item_id IS NOT NULL`,
  // migrate: copy main_item_id จาก med_subwarehouse → med_table (ข้อมูลเดิม)
  `UPDATE ${SCHEMA}.med_table mt
   SET main_item_id = ms.main_item_id
   FROM ${SCHEMA}.med_subwarehouse ms
   WHERE ms.med_id = mt.med_id
     AND ms.main_item_id IS NOT NULL
     AND mt.main_item_id IS NULL`,

  // ── prescriptions: คอลัมน์ที่ขาดหายไป ───────────────────────────────────────
  // source: ระบุแหล่งที่มาของใบสั่งยา ('counter' = สร้างจากระบบนี้, ค่า default)
  `ALTER TABLE ${SCHEMA}.prescriptions ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'counter'`,
  // queue_number: หมายเลขคิวที่ผูกกับใบสั่งยานี้ (บันทึกหลังจ่ายยา)
  `ALTER TABLE ${SCHEMA}.prescriptions ADD COLUMN IF NOT EXISTS queue_number VARCHAR(10)`,
];

async function migrate() {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO ${SCHEMA}, public`);
    console.log(`🔄 Running migrations on schema: ${SCHEMA} ...`);
    for (let i = 0; i < migrations.length; i++) {
      await client.query(migrations[i]);
      process.stdout.write(`  [${i + 1}/${migrations.length}] ✓\r`);
    }
    console.log(`\n✅ All ${migrations.length} migrations completed.`);
  } catch (err) {
    console.error('❌ Migration failed:', (err as Error).message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

migrate();

