import { Request, Response, NextFunction } from 'express';
import { execFile } from 'child_process';
import * as path from 'path';

// ── C# helper: render text → 1bpp bitmap → TSPL BITMAP command → WinSpool ─────
//
// แนวคิด: font built-in ของ TSPL printer ไม่รองรับภาษาไทย
// แก้โดย render ข้อความเป็น bitmap ก่อนด้วย System.Drawing (รองรับ font ไทยทุกตัว
// ที่ติดตั้งบน Windows) แล้วส่งเป็น TSPL BITMAP command แทน TEXT command

const LABEL_PRINTER_CS = `
using System;
using System.Drawing;
using System.Drawing.Text;
using System.Runtime.InteropServices;

public class LabelPrinter {
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)]
    struct DOCINFO { public string pDocName; public string pOutputFile; public string pDataType; }
    [DllImport("winspool.drv", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern bool OpenPrinter(string name, out IntPtr h, IntPtr def);
    [DllImport("winspool.drv", SetLastError=true)] static extern bool ClosePrinter(IntPtr h);
    [DllImport("winspool.drv", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern int StartDocPrinter(IntPtr h, int lvl, ref DOCINFO di);
    [DllImport("winspool.drv", SetLastError=true)] static extern bool EndDocPrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)] static extern bool StartPagePrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)] static extern bool EndPagePrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)]
    static extern bool WritePrinter(IntPtr h, byte[] buf, int len, out int written);

    // เลือก font ที่รองรับภาษาไทย: TH Sarabun New → Tahoma → Arial
    static string PickThaiFont() {
        var preferred = new[] { "TH Sarabun New", "Tahoma", "Arial Unicode MS", "Arial" };
        var installed = new System.Collections.Generic.HashSet<string>();
        foreach (var fam in FontFamily.Families) installed.Add(fam.Name);
        foreach (var name in preferred)
            if (installed.Contains(name)) return name;
        return "Arial";
    }

    public static void PrintLabel(string printerName, string[] lines, int mmW, int mmH, int dpi) {
        int pxW = (int)(mmW / 25.4 * dpi);
        int pxH = (int)(mmH / 25.4 * dpi);
        int widthBytes = (int)Math.Ceiling(pxW / 8.0);
        float fontSize = 32f;
        float lineH   = 38f;

        // 1. Render ข้อความเป็น bitmap
        byte[] bitmapData = new byte[widthBytes * pxH];
        using (var bmp = new Bitmap(pxW, pxH))
        using (var g = Graphics.FromImage(bmp)) {
            g.Clear(Color.White);
            g.TextRenderingHint = TextRenderingHint.AntiAlias;
            string fontName = PickThaiFont();
            using (var font  = new Font(fontName, fontSize, FontStyle.Bold, GraphicsUnit.Pixel))
            using (var brush = new SolidBrush(Color.Black)) {
                float y = 8f;
                foreach (var line in lines) {
                    if (line.Length > 0) g.DrawString(line, font, brush, 20f, y);
                    y += lineH;
                }
            }
            // 2. แปลง bitmap → 1bpp (MSB first) สำหรับ TSPL BITMAP
            for (int row = 0; row < pxH; row++)
                for (int col = 0; col < pxW; col++) {
                    Color px = bmp.GetPixel(col, row);
                    if ((int)(0.299*px.R + 0.587*px.G + 0.114*px.B) < 128)
                        bitmapData[row*widthBytes + col/8] |= (byte)(128 >> (col%8));
                }
            // printer นี้ใช้ bit=0 = พิมพ์ (ดำ), bit=1 = ไม่พิมพ์ (ขาว) → invert ทุก byte
            for (int i = 0; i < bitmapData.Length; i++)
                bitmapData[i] ^= 0xFF;
        }

        // 3. สร้าง TSPL binary: header (ASCII) + bitmap data (binary) + footer (ASCII)
        var enc  = System.Text.Encoding.ASCII;
        byte[] header = enc.GetBytes(
            "SIZE " + mmW + " mm, " + mmH + " mm\\r\\n" +
            "GAP 2 mm, 0 mm\\r\\n" +
            "SET TEAR ON\\r\\n" +
            "CLS\\r\\n" +
            "BITMAP 0,0," + widthBytes + "," + pxH + ",0,");
        byte[] footer = enc.GetBytes("\\r\\nPRINT 1,1\\r\\nEND\\r\\n");

        byte[] tspl = new byte[header.Length + bitmapData.Length + footer.Length];
        Array.Copy(header,     0, tspl, 0,                                  header.Length);
        Array.Copy(bitmapData, 0, tspl, header.Length,                      bitmapData.Length);
        Array.Copy(footer,     0, tspl, header.Length + bitmapData.Length,  footer.Length);

        // 4. ส่งผ่าน WinSpool RAW (ไม่ต้อง Share printer)
        IntPtr h;
        if (!OpenPrinter(printerName, out h, IntPtr.Zero))
            throw new Exception("OpenPrinter failed: " + Marshal.GetLastWin32Error());
        try {
            var di = new DOCINFO { pDocName = "TSPL Label", pOutputFile = null, pDataType = "RAW" };
            if (StartDocPrinter(h, 1, ref di) == 0) throw new Exception("StartDocPrinter failed");
            StartPagePrinter(h);
            int written;
            WritePrinter(h, tspl, tspl.Length, out written);
            EndPagePrinter(h);
            EndDocPrinter(h);
        } finally { ClosePrinter(h); }
    }
}`;

// ── PowerShell runner ─────────────────────────────────────────────────────────

function runPS(command: string): Promise<string> {
  const pwsh = path.join(
    process.env.windir ?? 'C:\\Windows',
    'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe'
  );
  const args = [
    '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
    '-Command',
    '[Console]::OutputEncoding=[Text.Encoding]::UTF8; ' +
    "$ErrorActionPreference='Stop'; " + command,
  ];
  return new Promise((resolve, reject) => {
    execFile(pwsh, args, { windowsHide: true, maxBuffer: 4 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      resolve(stdout.toString());
    });
  });
}

// ── Print ─────────────────────────────────────────────────────────────────────

function printDirect(lines: string[], printerName: string): Promise<void> {
  const b64Lines  = Buffer.from(JSON.stringify(lines), 'utf8').toString('base64');
  const safeName  = printerName.replace(/'/g, "''");

  const ps = `
Add-Type -AssemblyName System.Drawing
Add-Type -TypeDefinition @'
${LABEL_PRINTER_CS}
'@ -ReferencedAssemblies 'System.Drawing'

# ── ตรวจสอบเครื่องพิมพ์ก่อน submit ──────────────────────────────────
$prn = Get-Printer -Name '${safeName}' -ErrorAction Stop

if ($prn.WorkOffline) {
    throw "เครื่องพิมพ์ถูกตั้งเป็น Offline กรุณาตรวจสอบการเชื่อมต่อ"
}

# USB / locally-attached: ตรวจ PnP status
$pnp = Get-PnpDevice -Class Printer -ErrorAction SilentlyContinue |
       Where-Object { $_.FriendlyName -eq '${safeName}' } |
       Select-Object -First 1
if ($pnp -and $pnp.Status -ne 'OK') {
    throw "เครื่องพิมพ์ไม่พร้อม (PnP Status: $($pnp.Status)) — กรุณาตรวจสอบการเชื่อมต่อ"
}

# Network printer: ตรวจ TCP port 9100
$port = Get-PrinterPort -Name $prn.PortName -ErrorAction SilentlyContinue
if ($port -and $port.PrinterHostAddress) {
    $ok = Test-NetConnection -ComputerName $port.PrinterHostAddress -Port 9100 \`
          -WarningAction SilentlyContinue -InformationLevel Quiet
    if (-not $ok) {
        throw "ไม่สามารถเชื่อมต่อเครื่องพิมพ์ที่ $($port.PrinterHostAddress):9100"
    }
}

# ── พิมพ์ ─────────────────────────────────────────────────────────────
$lines = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${b64Lines}')) | ConvertFrom-Json
[LabelPrinter]::PrintLabel('${safeName}', [string[]]$lines, 60, 60, 203)
`;

  return runPS(ps).then(() => void 0);
}

// ── Printer list ──────────────────────────────────────────────────────────────

async function listPrinters(): Promise<object[]> {
  const ps = `
$pnp = Get-PnpDevice -Class Printer -ErrorAction SilentlyContinue |
  Select-Object FriendlyName, InstanceId, Status;

$spooler = @{};
try {
  $sp = Get-Printer -ErrorAction SilentlyContinue |
    Select-Object Name, DriverName, PortName, ShareName, PrinterStatus;
  foreach ($p in $sp) { $spooler[$p.Name] = $p }
} catch {}

$ports = @{};
try {
  $pp = Get-PrinterPort -ErrorAction SilentlyContinue |
    Select-Object Name, PortMonitor, PrinterHostAddress;
  foreach ($p in $pp) { $ports[$p.Name] = $p }
} catch {}

$out = foreach ($dev in $pnp) {
  $sp   = $spooler[$dev.FriendlyName];
  $port = if ($sp) { $ports[$sp.PortName] } else { $null };
  [PSCustomObject]@{
    Name          = $dev.FriendlyName;
    Status        = $dev.Status;
    InstanceId    = $dev.InstanceId;
    DriverName    = if ($sp) { $sp.DriverName }    else { $null };
    PortName      = if ($sp) { $sp.PortName }      else { $null };
    ShareName     = if ($sp) { $sp.ShareName }     else { $null };
    PrinterStatus = if ($sp) { $sp.PrinterStatus } else { $null };
    Port = if ($port) {
      [PSCustomObject]@{
        Name        = $port.Name;
        PortMonitor = $port.PortMonitor;
        HostAddress = $port.PrinterHostAddress;
      }
    } else { $null }
  }
}
@($out) | ConvertTo-Json -Depth 5 -Compress
`;

  const raw = await runPS(ps).catch(() => '');
  const trimmed = raw.trim();
  if (!trimmed) return [];
  try {
    const data = JSON.parse(trimmed);
    return Array.isArray(data) ? data : [data];
  } catch {
    return [];
  }
}

// ── Controllers ───────────────────────────────────────────────────────────────

export async function printLabel(req: Request, res: Response, next: NextFunction) {
  try {
    const { text, printerName } = req.body || {};
    if (!text || !printerName)
      return res.status(400).json({ error: 'กรอก text และ printerName' });

    const lines = String(text).split(/\r?\n/);
    console.log(`🖨  print @ ${new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' })}`, { printerName, lines: lines.length });
    await printDirect(lines, String(printerName));
    res.json({ ok: true, message: 'printed' });
  } catch (err) { next(err); }
}

export async function getPrinters(req: Request, res: Response, next: NextFunction) {
  try {
    console.log(`🖨  printers @ ${new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' })}`);
    const printers = await listPrinters();
    res.json(printers);
  } catch (err) { next(err); }
}
