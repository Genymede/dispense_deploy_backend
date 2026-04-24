/**
 * Seed: ข้อมูลเริ่มต้นสำหรับระบบคลังยาย่อย
 * รันด้วย: npm run db:seed
 */
import * as bcrypt from 'bcryptjs';
import { pool, SCHEMA } from './pool';
import dotenv from 'dotenv';
dotenv.config();

async function seed() {
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO ${SCHEMA}, public`);
    console.log('🌱 Seeding initial data...');

    // roles
    await client.query(`
      INSERT INTO ${SCHEMA}.roles (role_id, role_name, role_name_th, role_name_en)
      VALUES
        (1, 'admin',       'ผู้ดูแลระบบ',     'Administrator'),
        (2, 'pharmacist',  'เภสัชกร',          'Pharmacist'),
        (3, 'pharmacist_assistant', 'ผู้ช่วยเภสัชกร', 'Pharmacist Assistant'),
        (4, 'doctor',      'แพทย์',             'Doctor'),
        (5, 'nurse',       'พยาบาล',            'Nurse')
      ON CONFLICT (role_id) DO NOTHING;
    `);

    // users — hash รหัสผ่านจริงด้วย bcrypt (password เริ่มต้น: pharmsub1234)
    const defaultPassword = process.env.SEED_PASSWORD || 'pharmsub1234';
    const hash = await bcrypt.hash(defaultPassword, 10);
    console.log(`   ↳ hashing password for seed users...`);

    const seedUsers = [
      { username: 'admin',       email: 'admin@hospital.go.th',   phone: '021234560', role_id: 1 },
      { username: 'pharmacist1', email: 'pharma1@hospital.go.th', phone: '021234561', role_id: 2 },
      { username: 'pharmacist2', email: 'pharma2@hospital.go.th', phone: '021234562', role_id: 2 },
      { username: 'doctor1',     email: 'doctor1@hospital.go.th', phone: '021234563', role_id: 4 },
    ];

    for (const u of seedUsers) {
      await client.query(
        `INSERT INTO ${SCHEMA}.users (username, password, email, phone, role_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (username) DO UPDATE SET password = EXCLUDED.password`,
        [u.username, hash, u.email, u.phone, u.role_id]
      );
    }
    console.log(`   ↳ seed users: admin, pharmacist1, pharmacist2, doctor1 (password: ${defaultPassword})`);

    // sub_warehouse
    await client.query(`
      INSERT INTO ${SCHEMA}.sub_warehouse (name, description, is_active)
      VALUES
        ('คลังยาย่อย OPD', 'คลังยาแผนกผู้ป่วยนอก', true),
        ('คลังยาย่อย IPD', 'คลังยาแผนกผู้ป่วยใน', true),
        ('คลังยาย่อย ER',  'คลังยาแผนกฉุกเฉิน',   true)
      ON CONFLICT DO NOTHING;
    `);

    console.log('✅ Seed completed.');
  } catch (err) {
    console.error('❌ Seed failed:', err);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

seed();
