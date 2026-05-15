# Paperless Ingest

A self-hosted web UI for scanning documents directly into [Paperless-ngx](https://docs.paperless-ngx.com/).  
Upload files or trigger SANE-compatible scanners (ADF, Flatbed) — documents land in the Paperless consumption directory and get auto-processed within seconds.

## Features

- 📤 **Drag-and-drop upload** — PDF, PNG, JPG, TIFF
- 🖨 **SANE scan** — ADF and Flatbed with dynamic driver detection
- 🔁 **Duplex ADF** — Scan both sides, auto-interleave pages in correct order
- 🎛 **Driver-aware dropdowns** — Resolution, mode, and source auto-populated from the scanner
- 📦 **Paperless-ngx integration** — Saves directly to consume directory, auto-ingested
- ⚡ **Single-file** — No database, no build step, nothing to compile

## Quick Start

```bash
# Install dependencies
pip install fastapi uvicorn img2pdf pillow

# Install SANE (Linux)
sudo apt install sane-utils sane-airscan

# Run
python paperless_ingest.py

# Open http://localhost:3095
```

### systemd (production)

```bash
sudo cp paperless-ingest.service /etc/systemd/system/
# Edit the service file to set User=your_user and correct Python path
sudo systemctl daemon-reload
sudo systemctl enable --now paperless-ingest
```

## Requirements

- Python 3.10+
- `scanimage` from `sane-utils` (for scanner support)
- Paperless-ngx with a consumption directory (default: `/mnt/apple_xfs/documents/consume`)

### Python packages

```
fastapi
uvicorn
img2pdf
Pillow
```

## Configuration

Edit the `CONFIG` section at the top of `paperless_ingest.py`:

| Variable | Default | Description |
|---|---|---|
| `CONSUME_DIR` | `/mnt/apple_xfs/documents/consume` | Paperless consumption directory |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `3095` | HTTP port |

## API

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Web UI |
| `/api/scanners` | GET | List SANE scanners with capabilities |
| `/upload` | POST | Upload files (multipart `files`) |
| `/scan` | POST | Scan (params: `device`, `source`, `mode`, `resolution`) |
| `/scan/duplex/start` | POST | Start two-sided scan |
| `/scan/duplex/finish` | POST | Complete two-sided scan (params: `token`) |
| `/status` | GET | Last ingest info |

## License

MIT
