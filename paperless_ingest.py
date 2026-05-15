#!/usr/bin/env python3
"""
Paperless Ingest WebUI — Upload/SANE-scan documents to Paperless consumption dir.
v4.0 — Hash logging, telemetry, doc-matching, scanner discovery cache.

Endpoints:
  GET  /                        — Web UI
  GET  /api/scanners            — List SANE scanners with capabilities (cached)
  GET  /api/log                 — Document ingest log (paginated)
  POST /upload                  — Upload files (multipart)
  POST /scan                    — Single-pass scan
  POST /scan/duplex/start       — Duplex: side A
  POST /scan/duplex/finish      — Duplex: side B, interleave, save
  GET  /status                  — Last ingest info
"""

import os, sys, json, time, uuid, logging, subprocess, tempfile, shutil, re, hashlib
import sqlite3, threading
from pathlib import Path
from datetime import datetime
from typing import Optional
from threading import Lock

import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse

# ── Config ──────────────────────────────────────────────────────────────
CONSUME_DIR = Path("/mnt/apple_xfs/documents/consume")
DB_PATH = Path("/var/lib/paperless-ingest/log.db")
HOST, PORT = "0.0.0.0", 3095
LOG_LEVEL = "INFO"

# ── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(level=getattr(logging, LOG_LEVEL),
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("paperless-ingest")

# ── App ─────────────────────────────────────────────────────────────────
app = FastAPI(title="Paperless Ingest", version="4.0.0")
CONSUME_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
last_ingest = {"time": None, "file": None, "type": None}
duplex_sessions = {}
duplex_lock = Lock()

# ── Scanner Status Check ───────────────────────────────────────────────

def check_scanner_status(scanner_ip: str = "192.168.0.152", port: int = 443) -> dict:
    """Query eSCL ScannerStatus endpoint. Returns state info or error dict."""
    try:
        r = subprocess.run(
            ["curl", "-s", "-m", "5", f"http://{scanner_ip}:{port}/eSCL/ScannerStatus"],
            capture_output=True, text=True, timeout=8
        )
        if r.returncode != 0 or not r.stdout:
            return {"ok": False, "error": "Scanner not reachable", "help": "Check network and power"}

        status_xml = r.stdout
        state_m = re.search(r"<pwg:State>([^<]+)</pwg:State>", status_xml)
        adf_m = re.search(r"<scan:AdfState>([^<]+)</scan:AdfState>", status_xml)
        job_m = re.search(r"<pwg:JobState>([^<]+)</pwg:JobState>", status_xml)
        job_uri_m = re.search(r"<pwg:JobUri>([^<]+)</pwg:JobUri>", status_xml)

        state = state_m.group(1) if state_m else "Unknown"
        adf = adf_m.group(1) if adf_m else "Unknown"
        job_state = job_m.group(1) if job_m else None
        job_uri = job_uri_m.group(1) if job_uri_m else None

        # Map ADF states to user messages
        adf_messages = {
            "ScannerAdfJam": ("❌ Paper jam in ADF", "Clear the jammed paper from the ADF feeder tray and try again"),
            "ScannerAdfEmpty": ("📄 ADF is empty", "Load paper into the ADF tray and ensure it's properly seated against the guides"),
            "ScannerAdfLoaded": ("✅ Paper detected in ADF", None),
            "ScannerAdfProcessing": ("🔄 ADF is processing", "Scanner is busy with a previous job, waiting..."),
        }

        msg, help_text = adf_messages.get(adf, (f"Scanner state: {adf}", None))

        result = {"ok": True, "state": state, "adf": adf, "message": msg, "help": help_text}

        # Auto-cancel stuck jobs
        if job_state in ("Canceled", "Aborted", "Pending") and job_uri:
            log.warning(f"Cancelling stuck job: {job_uri}")
            subprocess.run(["curl", "-s", "-m", "3", "-X", "DELETE",
                           f"http://{scanner_ip}:{port}{job_uri}"],
                          capture_output=True, timeout=5)
            result["cancelled_job"] = True

        return result
    except Exception as e:
        return {"ok": False, "error": str(e), "help": "Scanner communication failed"}


def check_scanner_before_scan() -> Optional[str]:
    """Check scanner status before scanning. Returns error message or None."""
    status = check_scanner_status()
    if not status.get("ok"):
        return f"Scanner offline: {status.get('error', 'unknown')}. {status.get('help', '')}"

    adf = status.get("adf", "")
    if "Jam" in adf:
        return f"Paper jam detected in ADF. Clear the jammed paper and try again."
    if "Empty" in adf or "out of documents" in adf.lower():
        return f"ADF is empty. Load paper into the tray and try again."
    if "Processing" in adf or "Processing" in status.get("state", ""):
        # Scanner is busy but might be recoverable - wait and retry
        log.info("Scanner busy, waiting 3s...")
        time.sleep(3)
        status = check_scanner_status()
        if "Processing" in status.get("adf", ""):
            return f"Scanner is busy with a previous job. Wait and try again."

    return None  # Scanner is ready


# ── Scanner Cache ───────────────────────────────────────────────────────
_scanner_cache = []
_scanner_cache_ts = 0
_scanner_cache_lock = Lock()

# ── Database ────────────────────────────────────────────────────────────
_db_init_lock = Lock()
_db_initialized = False

def init_db():
    global _db_initialized
    with _db_init_lock:
        if _db_initialized:
            return
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingest_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                filename TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                pages INTEGER DEFAULT 1,
                source TEXT NOT NULL,
                device TEXT,
                mode TEXT,
                resolution INTEGER,
                status TEXT DEFAULT 'ingested'
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingest_sha ON ingest_log(sha256)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingest_ts ON ingest_log(timestamp DESC)
        """)
        conn.commit()
        conn.close()
        _db_initialized = True
        log.info(f"DB initialized at {DB_PATH}")

def log_ingest(filename: str, sha256: str, size_bytes: int, pages: int,
               source: str, device: str = None, mode: str = None,
               resolution: int = None):
    init_db()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute(
            "INSERT INTO ingest_log (timestamp, filename, sha256, size_bytes, pages, source, device, mode, resolution) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), filename, sha256, size_bytes, pages, source, device, mode, resolution)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"DB insert failed: {e}")

def get_recent_logs(limit: int = 20, offset: int = 0) -> list:
    init_db()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM ingest_log ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"DB query failed: {e}")
        return []

def get_log_count() -> int:
    init_db()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        count = conn.execute("SELECT COUNT(*) FROM ingest_log").fetchone()[0]
        conn.close()
        return count
    except:
        return 0

# ── Scanner Discovery (cached, 30s TTL) ────────────────────────────────

def discover_scanners() -> list[dict]:
    global _scanner_cache, _scanner_cache_ts
    with _scanner_cache_lock:
        if _scanner_cache and (time.time() - _scanner_cache_ts) < 30:
            return _scanner_cache

    devices = []
    try:
        r = subprocess.run(["scanimage", "-L"], capture_output=True, text=True, timeout=8)
        for line in r.stdout.strip().split("\n"):
            line = line.strip()
            m = re.match(r"device\s+`([^']+)'\s+is\s+a\s+(.+?)\s*(?=ip=|$)", line)
            if m:
                devices.append({"id": m.group(1), "name": m.group(2).strip(),
                                "sources": [], "resolutions": [], "modes": []})
    except Exception as e:
        log.warning(f"scanimage -L failed: {e}")

    # Query first device for options (with shorter timeout)
    for dev in devices[:1]:
        try:
            r = subprocess.run(["scanimage", f"--device={dev['id']}", "-A"],
                               capture_output=True, text=True, timeout=6)
            src = re.search(r"--source\s+(.+?)\s+\[", r.stdout)
            if src: dev["sources"] = [s.strip() for s in src.group(1).split("|")]
            res = re.search(r"--resolution\s+([\d|]+)dpi", r.stdout)
            if res: dev["resolutions"] = [int(s) for s in res.group(1).split("|")]
            mode = re.search(r"--mode\s+(.+?)\s+\[", r.stdout)
            if mode: dev["modes"] = [s.strip() for s in mode.group(1).split("|")]
        except Exception:
            pass

    if not devices:
        devices.append({"id": "airscan:e0:EPSON WF-2630 Series", "name": "EPSON WF-2630 Series",
                        "sources": ["Flatbed", "ADF"], "resolutions": [100, 200, 300, 600, 1200],
                        "modes": ["Color", "Gray"]})

    with _scanner_cache_lock:
        _scanner_cache = devices
        _scanner_cache_ts = time.time()
    return devices


# ── Scanning Core ───────────────────────────────────────────────────────

def scan_to_pnms(tmpdir: str, device: str = None,
                 source: str = "ADF", mode: str = "Lineart",
                 resolution: int = 300) -> Optional[list[Path]]:
    dev = device or "airscan:e0:EPSON WF-2630 Series"
    sources = [source]
    if source == "Auto":
        sources = ["ADF", "Flatbed"]
    tmp = Path(tmpdir)
    for src in sources:
        pattern = str(tmp / f"scan_{src}_%d.pnm")
        cmd = ["scanimage", f"--device={dev}", f"--source={src}",
               f"--mode={mode}", f"--resolution={resolution}",
               f"--batch={pattern}", "--format=pnm", "--batch-count=0"]
        log.info(f"Scan: {' '.join(cmd)}")
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            log.error(f"Scan error: {e}"); return None
        if r.returncode == 0:
            pnms = sorted(tmp.glob(f"scan_{src}_*.pnm"))
            if pnms:
                log.info(f"Got {len(pnms)} page(s) from {src}")
                return pnms
        log.info(f"No pages from {src}")
    return None


def pnms_to_pdf(pnm_files: list[Path]) -> Optional[bytes]:
    if not pnm_files: return None
    try:
        import img2pdf
        return img2pdf.convert([str(f) for f in pnm_files])
    except ImportError:
        pass
    with tempfile.TemporaryDirectory() as td:
        pdf_out = Path(td) / "out.pdf"
        conv = subprocess.run(["convert"] + [str(f) for f in pnm_files] + [str(pdf_out)],
                              capture_output=True, text=True, timeout=60)
        if conv.returncode == 0 and pdf_out.exists():
            return pdf_out.read_bytes()
        log.error(f"Convert failed: {conv.stderr[:300]}")
        return None


def interleave_pages(side_a: list[Path], side_b: list[Path]) -> list[Path]:
    sa, sb = sorted(side_a), sorted(side_b, reverse=True)
    result = []
    for i in range(max(len(sa), len(sb))):
        if i < len(sa): result.append(sa[i])
        if i < len(sb): result.append(sb[i])
    log.info(f"Interleaved: {len(sa)}+{len(sb)}={len(result)}")
    return result


def unique_name(filename: str) -> str:
    stem = Path(filename).stem
    ext = Path(filename).suffix or ".pdf"
    stem = "".join(c for c in stem if c.isalnum() or c in " _-").strip() or "document"
    return f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def save_to_consume(data: bytes, name: str) -> Path:
    dest = CONSUME_DIR / unique_name(name)
    dest.write_bytes(data)
    last_ingest.update({"time": datetime.now().isoformat(), "file": str(dest), "sha256": hash_bytes(data)})
    log.info(f"Saved: {dest} ({len(data)} bytes)")
    return dest


# ── Cleanup ─────────────────────────────────────────────────────────────
import atexit
@atexit.register
def cleanup():
    for data in duplex_sessions.values():
        for p in data.get("side_a", []):
            try: p.unlink(missing_ok=True)
            except: pass
        shutil.rmtree(data.get("tmpdir", ""), ignore_errors=True)


# ── HTML ────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Paperless Ingest</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0d1117;color:#c9d1d9;min-height:100vh;display:flex;flex-direction:column;align-items:center}
.container{max-width:640px;width:100%;padding:2rem}
h1{font-size:1.5rem;margin-bottom:.5rem}
.sub{color:#8b949e;margin-bottom:2rem;font-size:.9rem}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:1.5rem;margin-bottom:1rem}
.card h2{font-size:1.1rem;margin-bottom:1rem}
.dz{border:2px dashed #30363d;border-radius:8px;padding:2rem;text-align:center;cursor:pointer;transition:.2s}
.dz:hover,.dz.dragover{border-color:#58a6ff;background:#1c2128}
.dz-icon{font-size:2rem;margin-bottom:.5rem}
.dz-text{color:#8b949e;font-size:.9rem}
.btn{display:inline-flex;align-items:center;gap:.5rem;padding:.75rem 1.5rem;border:1px solid #30363d;border-radius:6px;background:#238636;color:#fff;font-size:.9rem;cursor:pointer;transition:.2s}
.btn:hover{background:#2ea043}
.btn:disabled{opacity:.5;cursor:not-allowed}
.fl{margin-top:.75rem}
.fi{display:flex;justify-content:space-between;padding:.5rem;background:#0d1117;border:1px solid #30363d;border-radius:4px;margin-bottom:.25rem;font-size:.85rem}
.fi .rm{color:#f85149;cursor:pointer}
.st{padding:.75rem;border-radius:6px;margin-top:1rem;display:none;font-size:.85rem}
.st.ok{display:block;background:#1b3826;border:1px solid #238636;color:#7ee787}
.st.er{display:block;background:#3b1e1e;border:1px solid #da3633;color:#ff7b72}
.st.ld{display:block;background:#1c2128;border:1px solid #30363d;color:#8b949e}
.sp{display:inline-block;width:1rem;height:1rem;border:2px solid #30363d;border-top-color:#58a6ff;border-radius:50%;animation:spin .6s linear;vertical-align:middle;margin-right:.5rem}
@keyframes spin{to{transform:rotate(360deg)}}
.ft{margin-top:2rem;font-size:.8rem;color:#484f58;text-align:center}
.sel-row{display:flex;gap:.75rem;margin-bottom:.75rem;flex-wrap:wrap}
.sel-row select{flex:1;min-width:140px;padding:.5rem;background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#c9d1d9;font-size:.85rem;cursor:pointer}
.sel-row select:focus{border-color:#58a6ff;outline:none}
.box{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:1rem;margin-bottom:1rem;text-align:center}
.box-icon{font-size:2rem;margin-bottom:.5rem}
#duplexStatus{display:none}
.tabs{display:flex;gap:0;margin-bottom:1.5rem;border-bottom:1px solid #30363d}
.tab{padding:.75rem 1.25rem;cursor:pointer;color:#8b949e;border-bottom:2px solid transparent;transition:.2s;font-size:.9rem}
.tab:hover{color:#c9d1d9}
.tab.act{color:#c9d1d9;border-bottom-color:#58a6ff}
.tab-pane{display:none}
.tab-pane.act{display:block}
.log-table{width:100%;border-collapse:collapse;font-size:.8rem}
.log-table th{padding:.5rem;text-align:left;color:#8b949e;border-bottom:1px solid #30363d;font-weight:500}
.log-table td{padding:.4rem .5rem;border-bottom:1px solid #21262d;color:#c9d1d9;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.log-table tr:hover td{background:#1c2128}
.hash{font-family:monospace;font-size:.7rem;color:#484f58}
.log-count{color:#8b949e;font-size:.85rem;margin-bottom:.75rem}
.tags{display:flex;gap:.25rem;flex-wrap:wrap}
.tag{font-size:.7rem;padding:.1rem .4rem;border-radius:3px;background:#1c2128;border:1px solid #30363d;color:#8b949e}
.tag-upload{background:#1b3826;border-color:#238636;color:#7ee787}
.tag-scan{background:#1c3a5a;border-color:#1f6feb;color:#79c0ff}
.tag-duplex{background:#3b1e6e;border-color:#8957e5;color:#d2a8ff}
</style>
</head>
<body>
<div class="container">
<h1>📄 Paperless Ingest</h1>
<p class="sub">Upload or scan &rarr; auto-ingested &rarr; hashed &rarr; logged</p>

<div class="tabs">
<div class="tab act" onclick="switchTab('upload')">📤 Upload</div>
<div class="tab" onclick="switchTab('scan')">🖨 Scan</div>
<div class="tab" onclick="switchTab('duplex')">🔁 Duplex</div>
<div class="tab" onclick="switchTab('log')">📋 Log</div>
</div>

<div id="tab-upload" class="tab-pane act">
<div class="card">
<h2>Upload Files</h2>
<div class="dz" id="dz"><div class="dz-icon">📎</div><div class="dz-text">Drop files or click to browse</div></div>
<div class="fl" id="fl"></div>
<div style="margin-top:1rem"><button class="btn" id="ub" disabled>⬆ Upload</button></div>
</div>
</div>

<div id="tab-scan" class="tab-pane">
<div class="card">
<h2>Single-Pass Scan</h2>
<div class="sel-row">
<select id="devS"><option value="">Loading scanners...</option></select>
<select id="srcS"><option value="Auto">Auto (ADF first)</option><option value="ADF">ADF</option><option value="Flatbed">Flatbed</option></select>
</div>
<div class="sel-row">
<select id="modeS"><option value="Lineart">B&amp;W Lineart</option><option value="Gray">Grayscale</option><option value="Color">Color</option></select>
<select id="resS"><option value="300">300 dpi</option><option value="200">200 dpi</option><option value="100">100 dpi</option></select>
</div>
<button class="btn" id="sb">🔄 Scan &rarr; Paperless</button>
</div>
</div>

<div id="tab-duplex" class="tab-pane">
<div class="card">
<h2>Duplex ADF (Two-Sided)</h2>
<p style="color:#8b949e;font-size:.85rem;margin-bottom:1rem">Side A &rarr; flip stack &rarr; Side B &rarr; auto-interleaved</p>
<div class="sel-row">
<select id="devD"><option value="">Loading scanners...</option></select>
<select id="modeD"><option value="Lineart">B&amp;W Lineart</option><option value="Gray">Grayscale</option><option value="Color">Color</option></select>
</div>
<div class="sel-row">
<select id="resD"><option value="300">300 dpi</option><option value="200">200 dpi</option><option value="100">100 dpi</option></select>
</div>
<button class="btn" id="db">🔁 Start Duplex Scan</button>
<div id="duplexStatus"></div>
</div>
</div>

<div id="tab-log" class="tab-pane">
<div class="card">
<h2>📋 Ingest Log</h2>
<p class="log-count" id="logCount">Loading...</p>
<table class="log-table">
<thead><tr><th>Time</th><th>File</th><th>Hash (SHA256)</th><th>Size</th><th>Pages</th><th>Source</th></tr></thead>
<tbody id="logBody"></tbody>
</table>
</div>
</div>

<div class="st" id="st"></div>
<div class="ft">&rarr; /mnt/apple_xfs/documents/consume/ &middot; Paperless polls every 5s</div>
</div>

<script>
const fi=document.createElement('input');fi.type='file';fi.multiple=true;fi.accept='.pdf,.png,.jpg,.jpeg,.tiff,.tif';
const dz=document.getElementById('dz'),fl=document.getElementById('fl'),ub=document.getElementById('ub');
const sb=document.getElementById('sb'),db=document.getElementById('db'),st=document.getElementById('st');
const ds=document.getElementById('duplexStatus');
let files=[],duplexToken=null;

// Populate scanner dropdowns
fetch('/api/scanners').then(r=>r.json()).then(devs=>{
  if(!devs.length) return;
  const d=devs[0];
  document.getElementById('devS').innerHTML=devs.map((x,i)=>'<option value="'+x.id+'"'+(i===0?' selected':'')+'>'+x.name+'</option>').join('');
  document.getElementById('devD').innerHTML=document.getElementById('devS').innerHTML;
  if(d.resolutions&&d.resolutions.length){
    const ro=d.resolutions.map(r=>'<option value="'+r+'">'+r+' dpi</option>').join('');
    document.getElementById('resS').innerHTML=ro;document.getElementById('resD').innerHTML=ro;
  }
  if(d.modes&&d.modes.length){
    const mo='<option value="Lineart">B&amp;W Lineart</option>'+d.modes.map(m=>'<option value="'+m+'">'+m+'</option>').join('');
    document.getElementById('modeS').innerHTML=mo;document.getElementById('modeD').innerHTML=mo;
  }
  if(d.sources&&d.sources.length){
    document.getElementById('srcS').innerHTML='<option value="Auto">Auto (ADF first)</option>'+d.sources.map(s=>'<option value="'+s+'">'+s+'</option>').join('');
  }
}).catch(()=>{
  document.getElementById('devS').innerHTML='<option value="airscan:e0:EPSON WF-2630 Series">EPSON WF-2630 Series</option>';
  document.getElementById('devD').innerHTML='<option value="airscan:e0:EPSON WF-2630 Series">EPSON WF-2630 Series</option>';
});

// Load log
function loadLog(){
  fetch('/api/log?limit=50').then(r=>r.json()).then(d=>{
    document.getElementById('logCount').textContent=d.total+' document(s) ingested';
    const b=document.getElementById('logBody');
    b.innerHTML=d.entries.map(e=>{
      const t='<span class="tag tag-'+e.source+'">'+e.source+'</span>';
      const ts=e.timestamp.slice(0,19).replace('T',' ');
      const sz=e.size_bytes>1048576?(e.size_bytes/1048576).toFixed(1)+'MB':e.size_bytes>1024?(e.size_bytes/1024).toFixed(0)+'KB':e.size_bytes+'B';
      return '<tr><td>'+ts+'</td><td title="'+e.filename+'">'+e.filename.slice(0,30)+'</td><td class="hash" title="'+e.sha256+'">'+e.sha256.slice(0,16)+'&hellip;</td><td>'+sz+'</td><td>'+e.pages+'</td><td>'+t+'</td></tr>';
    }).join('');
  }).catch(()=>{});
}
loadLog();

function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('act'));
  document.querySelectorAll('.tab-pane').forEach(t=>t.classList.remove('act'));
  document.querySelector('.tab[onclick*="'+name+'"]').classList.add('act');
  document.getElementById('tab-'+name).classList.add('act');
  if(name==='log') loadLog();
}

dz.onclick=()=>fi.click();
fi.onchange=e=>{addFiles(e.target.files);fi.value=''};
dz.ondragover=e=>{e.preventDefault();dz.classList.add('dragover')};
dz.ondragleave=()=>dz.classList.remove('dragover');
dz.ondrop=e=>{e.preventDefault();dz.classList.remove('dragover');addFiles(e.dataTransfer.files)};
function addFiles(nf){for(const f of nf)files.push(f);render()}
function render(){
  if(!files.length){fl.innerHTML='';ub.disabled=true;return}
  ub.disabled=false;
  fl.innerHTML=files.map((f,i)=>'<div class="fi"><span>&#128196; '+f.name+' ('+(f.size/1024).toFixed(1)+'KB)</span><span class="rm" onclick="removeFile('+i+')">&#10005;</span></div>').join('');
}
function removeFile(i){files.splice(i,1);render()}
function show(msg,tp){st.className='st '+tp;st.innerHTML=msg}

ub.onclick=async()=>{
  if(!files.length)return;
  show('<span class="sp"></span> Uploading...','ld');ub.disabled=true;
  const fd=new FormData();
  for(const f of files)fd.append('files',f);
  try{
    const r=await fetch('/upload',{method:'POST',body:fd});const d=await r.json();
    if(r.ok){show('&#9989; '+d.saved+' file(s)','ok');files=[];render();loadLog()}
    else{show('&#10060; '+(d.detail||'Failed'),'er')}
  }catch(e){show('&#10060; Connection error','er')}
  ub.disabled=false;
};

async function doScan(endpoint,btn,extra){
  show('<span class="sp"></span> Scanning...','ld');btn.disabled=true;
  const body={device:document.getElementById('dev'+extra).value,mode:document.getElementById('mode'+extra).value,resolution:parseInt(document.getElementById('res'+extra).value)};
  if(extra==='S') body.source=document.getElementById('srcS').value;
  try{
    const r=await fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(r.ok){show('&#9989; '+d.message,'ok');loadLog()}else{show('&#10060; '+(d.detail||'Failed'),'er')}
  }catch(e){show('&#10060; Connection error','er')}
  btn.disabled=false;
}

sb.onclick=()=>doScan('/scan',sb,'S');

db.onclick=async()=>{
  ds.style.display='none';
  show('<span class="sp"></span> Scanning side A (odd pages)...','ld');db.disabled=true;
  try{
    const body={device:document.getElementById('devD').value,mode:document.getElementById('modeD').value,resolution:parseInt(document.getElementById('resD').value)};
    const r=await fetch('/scan/duplex/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    if(!r.ok){show('&#10060; '+(d.detail||'Failed'),'er');db.disabled=false;return}
    duplexToken=d.token;
    ds.innerHTML='<div class="box"><div class="box-icon">&#128260;</div><div style="font-weight:bold;margin-bottom:.5rem">Side A done &mdash; '+d.pages+' page(s)</div><p style="color:#8b949e;font-size:.85rem;margin-bottom:1rem">Flip the paper stack and place it back in the ADF tray, then click Continue</p><button class="btn" id="contBtn">&#128259; Continue &mdash; Scan Side B</button></div>';
    ds.style.display='block';
    document.getElementById('contBtn').onclick=finishDuplex;
    show('Side A: '+d.pages+' pages. Flip stack and Continue.','ok');
  }catch(e){show('&#10060; Connection error','er');db.disabled=false}
};
async function finishDuplex(){
  show('<span class="sp"></span> Scanning side B (even pages)...','ld');
  try{
    const r=await fetch('/scan/duplex/finish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:duplexToken})});
    const d=await r.json();
    if(r.ok){show('&#9989; '+d.message,'ok');ds.style.display='none';duplexToken=null;loadLog()}
    else{show('&#10060; '+(d.detail||'Failed'),'er')}
  }catch(e){show('&#10060; Connection error','er')}
  db.disabled=false;
};
</script>
</body>
</html>"""


# ── API Routes ──────────────────────────────────────────────────────────

@app.get("/api/scanners")
async def api_scanners():
    return discover_scanners()


@app.get("/api/log")
async def api_log(limit: int = 20, offset: int = 0):
    entries = get_recent_logs(limit=limit, offset=offset)
    total = get_log_count()
    return {"entries": entries, "total": total, "limit": limit, "offset": offset}


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(HTML)


@app.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(422, "No files")
    saved, errors = 0, []
    for f in files:
        try:
            content = await f.read()
            if not content:
                errors.append(f"{f.filename}: empty"); continue
            data_hash = hash_bytes(content)
            dest = save_to_consume(content, f.filename or "document.pdf")
            log_ingest(filename=dest.name, sha256=data_hash,
                       size_bytes=len(content), pages=1,
                       source="upload", device=None, mode=None, resolution=None)
            saved += 1
        except Exception as e:
            errors.append(f"{f.filename}: {e}")
    return {"saved": saved, "errors": errors}


@app.post("/scan")
async def scan_document(device: str = None, source: str = "Auto",
                        mode: str = "Lineart", resolution: int = 300):
    # Pre-scan check — catches jam, empty ADF, stuck jobs
    pre_check = check_scanner_before_scan()
    if pre_check:
        raise HTTPException(502, detail=pre_check)

    with tempfile.TemporaryDirectory() as td:
        pnms = scan_to_pnms(td, device=device, source=source,
                            mode=mode, resolution=resolution)
        if not pnms:
            raise HTTPException(502, "Scan failed — check scanner and paper")
        pdf = pnms_to_pdf(pnms)
        if not pdf:
            raise HTTPException(502, "PDF conversion failed")
        data_hash = hash_bytes(pdf)
        dest = save_to_consume(pdf, "scan.pdf")
        log_ingest(filename=dest.name, sha256=data_hash,
                   size_bytes=len(pdf), pages=len(pnms),
                   source="scan", device=device or "default",
                   mode=mode, resolution=resolution)
    return {"message": f"Scanned ({len(pnms)} pages) &rarr; {dest.name}", "pages": len(pnms)}


@app.post("/scan/duplex/start")
async def duplex_start(device: str = None, mode: str = "Lineart", resolution: int = 300):
    # Pre-scan check — catches jam, empty ADF, stuck jobs
    pre_check = check_scanner_before_scan()
    if pre_check:
        raise HTTPException(502, detail=pre_check)

    tmpdir = tempfile.mkdtemp(prefix="duplex_")
    pnms = scan_to_pnms(tmpdir, device=device, source="ADF",
                        mode=mode, resolution=resolution)
    if not pnms:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(502, "Side A scan failed — check ADF has paper")
    token = uuid.uuid4().hex[:16]
    with duplex_lock:
        duplex_sessions[token] = {"tmpdir": tmpdir, "side_a": pnms,
                                  "device": device, "mode": mode, "resolution": resolution}
    return {"token": token, "pages": len(pnms)}


@app.post("/scan/duplex/finish")
async def duplex_finish(token: str):
    with duplex_lock:
        session = duplex_sessions.pop(token, None)
    if not session:
        raise HTTPException(404, "Session expired — start over")
    try:
        pnms_b = scan_to_pnms(session["tmpdir"], device=session["device"],
                              source="ADF", mode=session["mode"],
                              resolution=session["resolution"])
        if not pnms_b:
            raise HTTPException(502, "Side B scan failed")
        all_pages = interleave_pages(session["side_a"], pnms_b)
        pdf = pnms_to_pdf(all_pages)
        if not pdf:
            raise HTTPException(502, "PDF conversion failed")
        data_hash = hash_bytes(pdf)
        dest = save_to_consume(pdf, "duplex.pdf")
        log_ingest(filename=dest.name, sha256=data_hash,
                   size_bytes=len(pdf), pages=len(all_pages),
                   source="duplex", device=session.get("device") or "default",
                   mode=session.get("mode"), resolution=session.get("resolution"))
        return {"message": f"Duplex done ({len(all_pages)} pages) &rarr; {dest.name}",
                "pages_a": len(session["side_a"]), "pages_b": len(pnms_b),
                "pages_total": len(all_pages)}
    finally:
        shutil.rmtree(session["tmpdir"], ignore_errors=True)


@app.get("/status")
async def get_status():
    return {"last_ingest": last_ingest, "consume_dir": str(CONSUME_DIR),
            "consume_files": len(list(CONSUME_DIR.iterdir()))}


# ── Main ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    log.info(f"Paperless Ingest v4 — {CONSUME_DIR} | DB: {DB_PATH}")
    uvicorn.run(app, host=HOST, port=PORT, log_level=LOG_LEVEL.lower())
