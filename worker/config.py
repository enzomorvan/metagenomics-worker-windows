import os
from pathlib import Path

import platform as _platform
COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://194.164.206.175/compute")
API_KEY = os.environ.get("API_KEY", "jhyPOTYST8E_xyjEAyRJ1LWrMRoZeE33kV6fW9pgIQA")
WORKER_NAME = os.environ.get("WORKER_NAME", _platform.node())

BASE_DIR = Path(os.environ.get("WORK_DIR", str(Path.home() / "distributed_compute")))
DB_DIR = BASE_DIR / "databases"
RESULTS_DIR = BASE_DIR / "results"
WORK_DIR = BASE_DIR / "workdir"
LOGS_DIR = BASE_DIR / "logs"

THREADS = int(os.environ.get("THREADS", "12"))
MIN_DISK_GB = int(os.environ.get("MIN_DISK_GB", "30"))
POLL_INTERVAL_S = 30
HEARTBEAT_INTERVAL_S = 60
