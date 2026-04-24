import { Request, Response, NextFunction } from 'express';
import * as bcrypt from 'bcryptjs';
import * as crypto from 'crypto';
import { query, SCHEMA } from '../db/pool';
import { AppError } from '../middleware/errorHandler';

// ── helpers ───────────────────────────────────────────────────────────────────
function generateToken(): string {
  return crypto.randomBytes(32).toString('hex');
}

/** ลบ session ที่หมดอายุออกจาก DB (เรียกตอน login เพื่อ cleanup) */
async function purgeExpiredSessions(): Promise<void> {
  await query(`DELETE FROM ${SCHEMA}.sessions WHERE expires_at < NOW()`);
}

// ── POST /auth/login ──────────────────────────────────────────────────────────
export async function login(req: Request, res: Response, next: NextFunction) {
  try {
    const { username, password } = req.body;
    if (!username || !password) throw new AppError('กรุณากรอก username และ password', 400);

    const { rows } = await query(
      `SELECT u.uid, u.username, u.password, u.email, u.phone,
              u.first_name, u.last_name, u.role_id, u.is_active,
              r.role_name_th, r.role_name_en
       FROM   ${SCHEMA}.users u
       JOIN   ${SCHEMA}.roles r ON r.role_id = u.role_id
       WHERE  (u.username = $1 OR u.email = $1)`,
      [username]
    );

    if (!rows.length) throw new AppError('ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง', 401);
    const user = rows[0];
    if (!user.is_active) throw new AppError('บัญชีนี้ถูกระงับการใช้งาน', 403);

    const ok = await bcrypt.compare(password, user.password);
    if (!ok) throw new AppError('ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง', 401);

    // อัปเดต last_login + cleanup expired sessions (background, ไม่ block response)
    await query(`UPDATE ${SCHEMA}.users SET last_login = NOW() WHERE uid = $1`, [user.uid]);
    purgeExpiredSessions().catch(() => {});

    const token = generateToken();
    const expiresAt = new Date(Date.now() + 8 * 60 * 60 * 1000); // 8 ชั่วโมง

    await query(
      `INSERT INTO ${SCHEMA}.sessions (token, uid, username, role_id, expires_at)
       VALUES ($1, $2, $3, $4, $5)`,
      [token, user.uid, user.username, user.role_id, expiresAt]
    );

    res.json({
      token,
      user: {
        uid:          user.uid,
        username:     user.username,
        first_name:   user.first_name,
        last_name:    user.last_name,
        email:        user.email,
        phone:        user.phone,
        role_id:      user.role_id,
        role_name_th: user.role_name_th,
        role_name_en: user.role_name_en,
      },
      expires_at: expiresAt.toISOString(),
    });
  } catch (err) { next(err); }
}

// ── POST /auth/logout ─────────────────────────────────────────────────────────
export async function logout(req: Request, res: Response, next: NextFunction) {
  try {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (token) {
      await query(`DELETE FROM ${SCHEMA}.sessions WHERE token = $1`, [token]);
    }
    res.json({ message: 'ออกจากระบบเรียบร้อย' });
  } catch (err) { next(err); }
}

// ── GET /auth/me ──────────────────────────────────────────────────────────────
export async function me(req: Request, res: Response, next: NextFunction) {
  try {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) throw new AppError('ไม่ได้เข้าสู่ระบบ', 401);

    // Supabase JWT (มี 3 ส่วนคั่นด้วย '.')
    if (token.split('.').length === 3) {
      let payload: any;
      try {
        payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
        if (!payload.sub) throw new Error();
      } catch {
        throw new AppError('Token ไม่ถูกต้อง', 401);
      }

      const userId: string  = payload.sub;
      const email: string   = payload.email ?? '';
      const roleId: number  = payload.app_metadata?.role?.id ?? 1;
      const roleName: string = payload.app_metadata?.role?.name ?? '';

      // ดึงข้อมูลชื่อจาก public.profiles
      const { rows: profileRows } = await query(
        `SELECT p.firstname_th, p.lastname_th, p.firstname_en, p.lastname_en,
                p.role_id, r.role_name_th, r.role_name_en
         FROM   public.profiles p
         LEFT JOIN public.roles r ON r.id = p.role_id
         WHERE  p.id = $1`,
        [userId]
      );
      const profile = profileRows[0];

      return res.json({
        id:           userId,
        email,
        firstname_th: profile?.firstname_th ?? email.split('@')[0],
        lastname_th:  profile?.lastname_th  ?? '',
        firstname_en: profile?.firstname_en ?? '',
        lastname_en:  profile?.lastname_en  ?? '',
        role_id:      profile?.role_id      ?? roleId,
        role_name_th: profile?.role_name_th ?? roleName,
        role_name_en: profile?.role_name_en ?? roleName,
      });
    }

    // Session token เดิม (hex) — fallback
    const { rows: sessionRows } = await query(
      `SELECT uid, username, role_id, expires_at
       FROM   ${SCHEMA}.sessions
       WHERE  token = $1`,
      [token]
    );
    if (!sessionRows.length) throw new AppError('ไม่ได้เข้าสู่ระบบ', 401);

    const session = sessionRows[0];
    if (new Date(session.expires_at) < new Date()) {
      await query(`DELETE FROM ${SCHEMA}.sessions WHERE token = $1`, [token]);
      throw new AppError('Session หมดอายุ กรุณาเข้าสู่ระบบใหม่', 401);
    }

    const { rows } = await query(
      `SELECT u.uid, u.username, u.first_name, u.last_name,
              u.email, u.phone, u.role_id, r.role_name_th, r.role_name_en
       FROM   ${SCHEMA}.users u
       JOIN   ${SCHEMA}.roles r ON r.role_id = u.role_id
       WHERE  u.uid = $1`,
      [session.uid]
    );
    if (!rows.length) throw new AppError('ไม่พบผู้ใช้', 404);
    res.json(rows[0]);
  } catch (err) { next(err); }
}

// ── GET /auth/users — รายชื่อ users (สำหรับ dropdown) ────────────────────────
export async function getUsers(req: Request, res: Response, next: NextFunction) {
  try {
    const { role_id } = req.query;
    const params: any[] = [];
    let where = 'WHERE au.deleted_at IS NULL';
    if (role_id) {
      params.push(Number(role_id));
      where += ` AND p.role_id = $${params.length}`;
    }

    const { rows } = await query(
      `SELECT au.id,
              au.email,
              COALESCE(p.firstname_th || ' ' || p.lastname_th, au.email) AS full_name,
              p.firstname_th, p.lastname_th,
              p.firstname_en, p.lastname_en,
              p.role_id,
              r.role_name_th, r.role_name_en
       FROM   auth.users au
       LEFT JOIN public.profiles p  ON p.id  = au.id
       LEFT JOIN public.roles    r  ON r.id  = p.role_id
       ${where}
       ORDER  BY r.id, p.firstname_th`,
      params
    );
    res.json(rows);
  } catch (err) { next(err); }
}

// ── middleware: ถอด JWT โดยตรง ไม่ upsert DB ─────────────────────────────────
export async function verifyToken(req: Request, res: Response, next: NextFunction) {
  try {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) { next(); return; }

    // Supabase JWT (มี 3 ส่วนคั่นด้วย '.')
    if (token.split('.').length === 3) {
      try {
        const payload = JSON.parse(
          Buffer.from(token.split('.')[1], 'base64url').toString('utf8')
        );
        const userId: string = payload.sub;  // UUID จาก auth.users.id
        const email: string  = payload.email ?? '';
        if (userId) {
          (req as any).currentUser = {
            id:      userId,
            email,
            role_id: payload.app_metadata?.role?.id ?? 1,
          };
        }
      } catch { /* JWT decode error — ไม่ block request */ }
      next();
      return;
    }

    // Session token เดิม (hex) — fallback
    const { rows } = await query(
      `SELECT uid, username, role_id, expires_at
       FROM   ${SCHEMA}.sessions
       WHERE  token = $1`,
      [token]
    );
    if (rows.length && new Date(rows[0].expires_at) > new Date()) {
      (req as any).currentUser = {
        id:       String(rows[0].uid),
        email:    rows[0].username,
        role_id:  rows[0].role_id,
      };
    }
    next();
  } catch {
    next();
  }
}
