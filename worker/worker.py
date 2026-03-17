#!/usr/bin/env python3
"""Distributed compute worker for metagenomics pipeline.

Registers with the coordinator, syncs databases, then enters a poll loop:
claim task → run process_sample → upload results → repeat.
"""

import atexit
import os
import signal
import sys
import threading
import time

from . import config
from . import uploader
from .executor import run_sample, collect_result_files

_shutdown = threading.Event()
_lock_file = None


def _acquire_lock():
    """Ensure only one worker runs per machine. Exits if another is already running."""
    global _lock_file
    lock_path = config.BASE_DIR / "worker.lock"
    config.BASE_DIR.mkdir(parents=True, exist_ok=True)

    try:
        import platform
        if platform.system() == "Windows":
            # Windows: try to create exclusively
            import msvcrt
            _lock_file = open(lock_path, "w")
            msvcrt.locking(_lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            # Linux/macOS: fcntl flock
            import fcntl
            _lock_file = open(lock_path, "w")
            fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)

        _lock_file.write(str(time.time()))
        _lock_file.flush()
        atexit.register(_release_lock)

    except (IOError, OSError):
        print("ERROR: Another worker is already running on this machine.")
        print("Close it first, or check for a stale worker.lock file in:")
        print(f"  {lock_path}")
        sys.exit(1)


def _release_lock():
    global _lock_file
    if _lock_file:
        try:
            _lock_file.close()
        except Exception:
            pass
        lock_path = config.BASE_DIR / "worker.lock"
        lock_path.unlink(missing_ok=True)


def _check_disk() -> float:
    """Return free disk space in GB on the workdir partition."""
    import shutil
    config.WORK_DIR.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(str(config.WORK_DIR))
    return usage.free / (1024 ** 3)


def _handle_signal(signum, frame):
    print("\nShutdown requested — will finish current task then exit.")
    _shutdown.set()


def _heartbeat_loop(worker_id: str, accession: str):
    """Background thread: send heartbeats while a task is running."""
    while not _shutdown.is_set():
        _shutdown.wait(config.HEARTBEAT_INTERVAL_S)
        if _shutdown.is_set():
            break
        uploader.send_heartbeat(worker_id)
        uploader.send_task_heartbeat(accession, worker_id)


def main():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    _acquire_lock()

    print(f"Worker: {config.WORKER_NAME}")
    print(f"Coordinator: {config.COORDINATOR_URL}")
    print(f"Threads: {config.THREADS}")
    print()

    # Register
    print("Registering with coordinator...")
    try:
        worker_id = uploader.register_worker()
    except Exception as e:
        print(f"Failed to register: {e}")
        print("Is the coordinator running? Check COORDINATOR_URL and API_KEY.")
        sys.exit(1)
    print(f"Registered as {worker_id}")

    # Sync databases
    try:
        uploader.sync_databases()
    except Exception as e:
        print(f"Database sync failed: {e}")
        sys.exit(1)

    print()
    print("Entering poll loop (Ctrl+C to stop after current task)...")
    print()

    while not _shutdown.is_set():
        # Heartbeat + check if enabled
        enabled = uploader.send_heartbeat(worker_id)
        if not enabled:
            print("Worker disabled by coordinator — waiting...")
            _shutdown.wait(config.POLL_INTERVAL_S)
            continue

        # Check for database updates before claiming
        try:
            if uploader.check_db_updates():
                print("Database update detected — re-syncing...")
                uploader.sync_databases()
                print("Database sync complete.")
        except Exception as e:
            print(f"DB update check failed (non-fatal): {e}")

        # Check disk space before claiming
        free_gb = _check_disk()
        if free_gb < config.MIN_DISK_GB:
            print(f"Low disk space: {free_gb:.1f} GB free (need {config.MIN_DISK_GB} GB) — waiting...")
            _shutdown.wait(config.POLL_INTERVAL_S)
            continue

        # Claim task
        try:
            task = uploader.claim_task(worker_id)
        except Exception as e:
            print(f"Failed to claim task: {e}")
            _shutdown.wait(config.POLL_INTERVAL_S)
            continue

        if not task:
            _shutdown.wait(config.POLL_INTERVAL_S)
            continue

        accession = task["accession"]
        study = task["study"]
        print(f"--- Processing {accession} ({study}) ---")

        # Start heartbeat thread
        hb_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(worker_id, accession),
            daemon=True,
        )
        hb_thread.start()

        # Execute
        success, duration_s, error, step_timings = run_sample(accession)

        if success:
            print(f"  Completed in {duration_s // 60}m {duration_s % 60}s")
            if step_timings:
                print(f"  Timings: {step_timings}")

            # Upload results
            result_files = collect_result_files(accession)
            if result_files:
                print(f"  Uploading {len(result_files)} result files...")
                try:
                    uploader.upload_results(accession, result_files)
                    print("  Upload complete")
                except Exception as e:
                    print(f"  Upload failed: {e}")

            # Mark complete
            try:
                uploader.complete_task(accession, worker_id, duration_s, step_timings)
            except Exception as e:
                print(f"  Failed to mark complete: {e}")
        else:
            print(f"  FAILED after {duration_s // 60}m {duration_s % 60}s")
            print(f"  Error: {error[:200]}")
            try:
                uploader.fail_task(accession, worker_id, error)
            except Exception as e:
                print(f"  Failed to report failure: {e}")

        print()

    print("Worker stopped.")


if __name__ == "__main__":
    main()
