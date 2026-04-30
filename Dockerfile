FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl sqlite3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python - <<'PY'
import json
import os
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

api = "https://api.github.com/repos/XTLS/Xray-core/releases/latest"
with urllib.request.urlopen(urllib.request.Request(api, headers={"User-Agent": "submanager-build"}), timeout=30) as r:
    release = json.load(r)

asset_url = None
for asset in release.get("assets", []):
    if asset.get("name") == "Xray-linux-64.zip":
        asset_url = asset.get("browser_download_url")
        break
if not asset_url:
    raise SystemExit("Xray-linux-64.zip not found in latest release")

tmpdir = tempfile.mkdtemp(prefix="xray-build-")
archive = Path(tmpdir) / "xray.zip"
with urllib.request.urlopen(urllib.request.Request(asset_url, headers={"User-Agent": "submanager-build"}), timeout=60) as r:
    archive.write_bytes(r.read())

with zipfile.ZipFile(archive) as zf:
    member = next((name for name in zf.namelist() if name.endswith("/xray") or name == "xray"), None)
    if not member:
        raise SystemExit("xray binary missing from archive")
    with zf.open(member) as src, open("/usr/local/bin/xray", "wb") as dst:
        shutil.copyfileobj(src, dst)

os.chmod("/usr/local/bin/xray", 0o755)
shutil.rmtree(tmpdir)
PY

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8080
EXPOSE 20000-24999
EXPOSE 25000-25999

CMD ["python", "-m", "submanager.main", "--config", "/app/config/config.yml"]
