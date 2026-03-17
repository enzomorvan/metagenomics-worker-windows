import hashlib
import time
from pathlib import Path

import requests

from . import config

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers["Authorization"] = f"Bearer {config.API_KEY}"
        adapter = requests.adapters.HTTPAdapter(
            max_retries=requests.adapters.Retry(
                total=5, backoff_factor=2,
                status_forcelist=[500, 502, 503, 504],
            )
        )
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)
    return _session


def api_url(path: str) -> str:
    return f"{config.COORDINATOR_URL}{path}"


def register_worker() -> str:
    """Register with coordinator, returns worker_id."""
    import platform
    import os

    is_wsl = "microsoft" in platform.release().lower()
    plat = "wsl2" if is_wsl else platform.system().lower()

    try:
        import psutil
        cpu_count = psutil.cpu_count(logical=True)
        ram_gb = psutil.virtual_memory().total / (1024**3)
    except ImportError:
        cpu_count = os.cpu_count() or 0
        ram_gb = 0

    resp = _get_session().post(api_url("/api/workers/register"), json={
        "name": config.WORKER_NAME,
        "hostname": platform.node(),
        "platform": plat,
        "cpu_count": cpu_count,
        "ram_gb": round(ram_gb, 1),
    })
    resp.raise_for_status()
    return resp.json()["worker_id"]


def _get_cpu_temp() -> float | None:
    """Read CPU temperature. Returns degrees C or None."""
    try:
        import psutil
        temps = psutil.sensors_temperatures()
        for name in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if name in temps and temps[name]:
                return max(t.current for t in temps[name])
        # Fallback: first available sensor
        for entries in temps.values():
            if entries:
                return max(t.current for t in entries)
    except Exception:
        pass
    # Windows: try WMI via subprocess
    try:
        import subprocess, platform
        if platform.system() == "Windows":
            out = subprocess.check_output(
                ["powershell", "-Command",
                 "Get-CimInstance MSAcpi_ThermalZoneTemperature -Namespace root/wmi 2>$null | Select -Expand CurrentTemperature | Select -First 1"],
                timeout=5, stderr=subprocess.DEVNULL, text=True,
            ).strip()
            if out:
                return round((int(out) / 10.0) - 273.15, 1)
    except Exception:
        pass
    # Linux fallback: read sysfs directly
    try:
        from pathlib import Path
        for tz in sorted(Path("/sys/class/thermal").glob("thermal_zone*/temp")):
            val = int(tz.read_text().strip())
            if val > 0:
                return round(val / 1000.0, 1)
    except Exception:
        pass
    return None


def _get_last_log() -> str | None:
    """Read the last 30 lines of the most recent log file."""
    try:
        log_dir = config.LOGS_DIR
        if not log_dir.exists():
            return None
        logs = sorted(log_dir.glob("*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not logs:
            return None
        lines = logs[0].read_text(errors="replace").splitlines()[-30:]
        return "\n".join(lines)
    except Exception:
        return None


def send_heartbeat(worker_id: str) -> bool:
    """Send heartbeat with CPU temp and log tail, returns whether worker is enabled."""
    try:
        temp = _get_cpu_temp()
        last_log = _get_last_log()
        resp = _get_session().post(
            api_url(f"/api/workers/{worker_id}/heartbeat"),
            json={"cpu_temp": temp, "last_log": last_log},
        )
        resp.raise_for_status()
        return resp.json()["enabled"]
    except Exception:
        return True  # assume enabled if coordinator unreachable


def claim_task(worker_id: str) -> dict | None:
    """Claim next pending task. Returns task dict or None."""
    resp = _get_session().post(api_url("/api/tasks/claim"), json={"worker_id": worker_id})
    resp.raise_for_status()
    return resp.json().get("task")


def complete_task(accession: str, worker_id: str, duration_s: int, step_timings: dict | None = None):
    resp = _get_session().post(api_url(f"/api/tasks/{accession}/complete"), json={
        "worker_id": worker_id,
        "duration_s": duration_s,
        "step_timings": step_timings,
    })
    resp.raise_for_status()


def fail_task(accession: str, worker_id: str, error_message: str):
    resp = _get_session().post(api_url(f"/api/tasks/{accession}/fail"), json={
        "worker_id": worker_id,
        "error_message": error_message[:2000],
    })
    resp.raise_for_status()


def send_task_heartbeat(accession: str, worker_id: str):
    try:
        _get_session().post(api_url(f"/api/tasks/{accession}/heartbeat"), json={
            "worker_id": worker_id,
        })
    except Exception:
        pass


def upload_results(accession: str, files: list[Path]):
    file_tuples = [("files", (f.name, open(f, "rb"), "text/tab-separated-values")) for f in files]
    try:
        resp = _get_session().post(api_url(f"/api/results/{accession}/upload"), files=file_tuples)
        resp.raise_for_status()
    finally:
        for _, (_, fh, _) in file_tuples:
            fh.close()


_last_manifest_sha: str | None = None


def check_db_updates() -> bool:
    """Quick check: has the coordinator's DB manifest changed since last sync?
    Returns True if databases need re-syncing."""
    global _last_manifest_sha
    sess = _get_session()
    resp = sess.get(api_url("/api/databases/manifest"))
    resp.raise_for_status()
    manifest = resp.json()

    # Hash the manifest to detect changes
    import json
    manifest_sha = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()

    if _last_manifest_sha is None:
        _last_manifest_sha = manifest_sha
        return False  # first check, assume already synced at startup

    if manifest_sha != _last_manifest_sha:
        _last_manifest_sha = manifest_sha
        return True

    return False


def sync_databases():
    """Download missing or outdated database files from coordinator."""
    config.DB_DIR.mkdir(parents=True, exist_ok=True)
    sess = _get_session()

    print("Checking database files...")
    resp = sess.get(api_url("/api/databases/manifest"))
    resp.raise_for_status()
    manifest = resp.json()

    for entry in manifest["files"]:
        rel_path = entry["path"]
        expected_sha = entry["sha256"]
        expected_size = entry["size"]

        local_path = config.DB_DIR / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if file exists and matches
        if local_path.exists():
            if local_path.stat().st_size == expected_size:
                local_sha = hashlib.sha256(local_path.read_bytes()).hexdigest()
                if local_sha == expected_sha:
                    print(f"  OK: {rel_path}")
                    continue

        # Download
        print(f"  Downloading: {rel_path} ({expected_size / 1024 / 1024:.1f} MB)...")
        resp = sess.get(api_url(f"/api/databases/file/{rel_path}"), stream=True)
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  Done: {rel_path}")

    print("Database sync complete.")


