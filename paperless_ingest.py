#!/usr/bin/env python3
"""
Paperless Ingest WebUI — Upload/SANE-scan documents to Paperless consumption dir.
v3.0 — Device/source/resolution/mode dropdowns, auto-detect scanners.

Endpoints:
  GET  /                        — Web UI
  GET  /api/scanners            — List all SANE scanners with capabilities
  POST /upload                  — Upload file(s) (multipart)
  POST /scan                    — Single-pass scan (device, source, mode, res)
  POST /scan/duplex/start       — Duplex: side A
  POST /scan/duplex/finish      — Duplex: side B, interleave, save
  GET  /status                  — Last ingest info
"""

import os, sys, json, time, uuid, logging, subprocess, tempfile, shutil, re
from pathlib import Path
from datetime import datetime
from typing import Optional
from threading import Lock

import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse

CONSUME_DIR = Path("/mnt/apple_xfs/documents/consume")
HOST, PORT = "0.0.0.0", 3095
LOG_LEVEL = "INFO"

logging.basicConfig(level=getattr(logging, LOG_LEVEL),
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("paperless-ingest")

app = FastAPI(title="Paperless Ingest", version="3.0.0")
CONSUME_DIR.mkdir(parents=True, exist_ok=True)
last_ingest = {"time": None, "file": None, "type": None}
duplex_sessions = {}
duplex_lock = Lock()


# ── Scanner Discovery ───────────────────────────────────────────────────

def discover_scanners() -> list[dict]:
    """Run scanimage -L and parse devices + query each for options."""
    devices = []
    try:
        r = subprocess.run(["scanimage", "-L"], capture_output=True, text=True, timeout=10)
        for line in r.stdout.strip().split("\n"):
            line = line.strip()
            m = re.match(r"device\s+`([^']+)'\s+is\s+a\s+(.+?)\s*(?=ip=|$)", line)
            if m:
                dev_id = m.group(1)
                desc = m.group(2).strip()
                devices.append({"id": dev_id, "name": desc, "sources": [], "resolutions": []})
    except Exception as e:
        log.warning(f"Scanner discovery failed: {e}")

    # Query each device for options
    for dev in devices[:1]:  # Only query first device (avoids timeouts)
        try:
            r = subprocess.run(["scanimage", f"--device={dev['id']}", "-A"],
                               capture_output=True, text=True, timeout=8)
            # Parse sources
            src_m = re.search(r"--source\s+(.+?)\s+\[", r.stdout)
            if src_m:
                dev["sources"] = [s.strip() for s in src_m.group(1).split("|")]
            # Parse resolutions
            res_m = re.search(r"--resolution\s+([\d|]+)dpi", r.stdout)
            if res_m:
                dev["resolutions"] = [int(s) for s in res_m.group(1).split("|")]
            # Parse modes
            mode_m = re.search(r"--mode\s+(.+?)\s+\[", r.stdout)
            if mode_m:
                dev["modes"] = [s.strip() for s in mode_m.group(1).split("|")]
        except Exception:
            pass

    if not devices:
        devices.append({"id": "airscan:e0:EPSON WF-2630 Series", "name": "EPSON WF-2630 Series",
                        "sources": ["Flatbed", "ADF"], "resolutions": [100, 200, 300, 600, 1200],
                        "modes": ["Color", "Gray"]})
    return devices


# ── Scanning Core ───────────────────────────────────────────────────────

def scan_to_pnms(tmpdir: str, device: str = None,
                 source: str = "ADF", mode: str = "Lineart",
                 resolution: int = 300) -> Optional[list[Path]]:
    """Scan from SANE device. Returns list of PNM paths or None."""
    if device is None:
        device = "airscan:e0:EPSON WF-2630 Series"

    # If source is "Auto", try ADF first, fall back to Flatbed
    sources_to_try = [source]
    if source == "Auto":
        sources_to_try = ["ADF", "Flatbed"]

    tmp = Path(tmpdir)
    for src in sources_to_try:
        pattern = str(tmp / f"scan_{src}_%d.pnm")
        cmd = ["scanimage", f"--device={device}", f"--source={src}",
               f"--mode={mode}", f"--resolution={resolution}",
               f"--batch={pattern}", "--format=pnm", "--batch-count=0"]
        log.info(f"Scan: {' '.join(cmd)}")
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            log.error(f"Scan error: {e}")
            return None
        if r.returncode == 0:
            pnms = sorted(tmp.glob(f"scan_{src}_*.pnm"))
            if pnms:
                log.info(f"Got {len(pnms)} page(s) from {src}")
                return pnms
        log.info(f"No pages from {src} (rc={r.returncode}), trying next..." if len(sources_to_try) > 1 else "")
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
    side_a = sorted(side_a)
    side_b = sorted(side_b, reverse=True)
    result = []
    for i in range(max(len(side_a), len(side_b))):
        if i < len(side_a): result.append(side_a[i])
        if i < len(side_b): result.append(side_b[i])
    log.info(f"Interleaved: {len(side_a)} + {len(side_b)} = {len(result)} pages")
    return result


def unique_name(filename: str) -> str:
    stem = Path(filename).stem
    ext = Path(filename).suffix or ".pdf"
    stem = "".join(c for c in stem if c.isalnum() or c in " _-").strip() or "document"
    return f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"


def save_to_consume(data: bytes, name: str) -> Path:
    dest = CONSUME_DIR / unique_name(name)
    dest.write_bytes(data)
    last_ingest.update({"time": datetime.now().isoformat(), "file": str(dest)})
    log.info(f"Saved: {dest} ({len(data)} bytes)")
    return dest


# ── Cleanup ─────────────────────────────────────────────────────────────
import atexit
@atexit.register
def cleanup():
    for token, data in list(duplex_sessions.items()):
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
</style>
</head>
<body>
<div class="container">
<h1>📄 Paperless Ingest</h1>
<p class="sub">Upload or scan &rarr; auto-ingested by Paperless-ngx</p>

<div class="tabs">
<div class="tab act" onclick="switchTab('upload')">📤 Upload</div>
<div class="tab" onclick="switchTab('scan')">🖨 Scan</div>
<div class="tab" onclick="switchTab('duplex')">🔁 Duplex</div>
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
<select id="resS"><option value="300">300 dpi</option><option value="200">200 dpi</option><option value="100">100 dpi</option><option value="600">600 dpi</option></select>
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
<select id="resD"><option value="300">300 dpi</option><option value="200">200 dpi</option><option value="100">100 dpi</option><option value="600">600 dpi</option></select>
</div>
<button class="btn" id="db">🔁 Start Duplex Scan</button>
<div id="duplexStatus"></div>
</div>
</div>

<div class="st" id="st"></div>
<div class="ft">&rarr; /mnt/apple_xfs/documents/consume/ &middot; Paperless polls every 5s</div>
</div>

<script>
const fi=document.createElement('input');fi.type='file';fi.multiple=true;fi.accept='.pdf,.png,.jpg,.jpeg,.tiff,.tif';
const dz=document.getElementById('dz'),fl=document.getElementById('fl'),ub=document.getElementById('ub');
const sb=document.getElementById('sb'),db=document.getElementById('db'),st=document.getElementById('status');
const ds=document.getElementById('duplexStatus');
let files=[],duplexToken=null;

// Populate scanner dropdowns
fetch('/api/scanners').then(r=>r.json()).then(devs=>{
  if(!devs.length) return;
  const d=devs[0];
  const devOpts=devs.map((x,i)=>'<option value="'+x.id+'"'+(i===0?' selected':'')+'>'+x.name+'</option>').join('');
  document.getElementById('devS').innerHTML=devOpts;
  document.getElementById('devD').innerHTML=devOpts;
  // Populate resolutions from driver
  if(d.resolutions&&d.resolutions.length){
    const resOpts=d.resolutions.map(r=>'<option value="'+r+'">'+r+' dpi</option>').join('');
    document.getElementById('resS').innerHTML=resOpts;
    document.getElementById('resD').innerHTML=resOpts;
  }
  // Populate modes from driver (keep Lineart as extra since SANE supports it)
  if(d.modes&&d.modes.length){
    const modeOpts='<option value="Lineart">B&amp;W Lineart</option>'+d.modes.map(m=>'<option value="'+m+'">'+m+(m==='Color'?'':'')+'</option>').join('');
    document.getElementById('modeS').innerHTML=modeOpts;
    document.getElementById('modeD').innerHTML=modeOpts;
  }
  // Update source options if driver reports sources
  if(d.sources&&d.sources.length){
    const srcOpts='<option value="Auto">Auto (ADF first)</option>'+d.sources.map(s=>'<option value="'+s+'">'+s+'</option>').join('');
    document.getElementById('srcS').innerHTML=srcOpts;
  }
}).catch(()=>{
  document.getElementById('devS').innerHTML='<option value="airscan:e0:EPSON WF-2630 Series">EPSON WF-2630 Series</option>';
  document.getElementById('devD').innerHTML='<option value="airscan:e0:EPSON WF-2630 Series">EPSON WF-2630 Series</option>';
});

function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('act'));
  document.querySelectorAll('.tab-pane').forEach(t=>t.classList.remove('act'));
  document.querySelector('.tab[onclick*="'+name+'"]').classList.add('act');
  document.getElementById('tab-'+name).classList.add('act');
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
    if(r.ok){show('&#9989; '+d.saved+' file(s)','ok');files=[];render()}
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
    if(r.ok){show('&#9989; '+d.message,'ok')}else{show('&#10060; '+(d.detail||'Failed'),'er')}
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
    if(r.ok){show('&#9989; '+d.message,'ok');ds.style.display='none';duplexToken=null}
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
            if content:
                save_to_consume(content, f.filename or "document.pdf")
                saved += 1
            else:
                errors.append(f"{f.filename}: empty")
        except Exception as e:
            errors.append(f"{f.filename}: {e}")
    return {"saved": saved, "errors": errors}


@app.post("/scan")
async def scan_document(device: str = None, source: str = "Auto",
                        mode: str = "Lineart", resolution: int = 300):
    with tempfile.TemporaryDirectory() as td:
        pnms = scan_to_pnms(td, device=device, source=source,
                            mode=mode, resolution=resolution)
        if not pnms:
            raise HTTPException(502, "Scan failed — check scanner and paper")
        pdf = pnms_to_pdf(pnms)
        if not pdf:
            raise HTTPException(502, "PDF conversion failed")
        dest = save_to_consume(pdf, "scan.pdf")
    return {"message": f"Scanned ({len(pnms)} pages) &rarr; {dest.name}", "pages": len(pnms)}


@app.post("/scan/duplex/start")
async def duplex_start(device: str = None, mode: str = "Lineart", resolution: int = 300):
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
        dest = save_to_consume(pdf, "duplex.pdf")
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
    log.info(f"Paperless Ingest v3 — {CONSUME_DIR}")
    uvicorn.run(app, host=HOST, port=PORT, log_level=LOG_LEVEL.lower())
