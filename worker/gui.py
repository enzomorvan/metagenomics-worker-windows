"""Simple GUI launcher for the metagenomics worker.

Lets the user pick thread count and start/stop the worker.
Uses tkinter (bundled with Python, no extra install).
"""

import os
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk

from . import config


def main():
    cpu_count = os.cpu_count() or 4

    root = tk.Tk()
    root.title("Metagenomics Worker")
    root.configure(bg="#1e1e2e")
    root.resizable(False, False)

    # Style
    fg = "#cdd6f4"
    bg = "#1e1e2e"
    bg2 = "#313244"
    green = "#a6e3a1"
    red = "#f38ba8"
    blue = "#89b4fa"

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TLabel", background=bg, foreground=fg, font=("Segoe UI", 11))
    style.configure("TButton", font=("Segoe UI", 11, "bold"), padding=8)
    style.configure("Header.TLabel", font=("Segoe UI", 16, "bold"), foreground=blue, background=bg)
    style.configure("Status.TLabel", font=("Segoe UI", 10), background=bg)
    style.configure("TScale", background=bg, troughcolor=bg2)
    style.configure("TFrame", background=bg)

    frame = ttk.Frame(root, padding=20)
    frame.pack()

    # Header
    ttk.Label(frame, text="Metagenomics Worker", style="Header.TLabel").grid(
        row=0, column=0, columnspan=2, pady=(0, 15))

    # Machine info
    ttk.Label(frame, text="Machine:").grid(row=1, column=0, sticky="w", pady=3)
    ttk.Label(frame, text=config.WORKER_NAME).grid(row=1, column=1, sticky="w", pady=3)

    ttk.Label(frame, text="CPU cores:").grid(row=2, column=0, sticky="w", pady=3)
    ttk.Label(frame, text=str(cpu_count)).grid(row=2, column=1, sticky="w", pady=3)

    ttk.Label(frame, text="Coordinator:").grid(row=3, column=0, sticky="w", pady=3)
    coord_text = config.COORDINATOR_URL.replace("http://", "")
    ttk.Label(frame, text=coord_text).grid(row=3, column=1, sticky="w", pady=3)

    # Thread selector
    ttk.Label(frame, text="Threads:").grid(row=4, column=0, sticky="w", pady=(15, 3))

    thread_frame = ttk.Frame(frame)
    thread_frame.grid(row=4, column=1, sticky="w", pady=(15, 3))

    default_threads = min(max(cpu_count - 2, 2), cpu_count)
    thread_var = tk.IntVar(value=default_threads)

    thread_label = ttk.Label(thread_frame, text=str(default_threads), width=3,
                             font=("Segoe UI", 14, "bold"), foreground=blue, background=bg)
    thread_label.pack(side="left", padx=(0, 10))

    def on_scale(val):
        v = int(float(val))
        thread_var.set(v)
        thread_label.configure(text=str(v))

    scale = ttk.Scale(thread_frame, from_=1, to=cpu_count, variable=thread_var,
                      orient="horizontal", length=200, command=on_scale)
    scale.pack(side="left")

    # Status
    status_var = tk.StringVar(value="Stopped")
    status_label = ttk.Label(frame, textvariable=status_var, style="Status.TLabel",
                             foreground=fg)
    status_label.grid(row=5, column=0, columnspan=2, pady=(15, 5))

    # Progress bar for current sample
    STEPS = ["Download", "QC", "NCycDB", "PlasticDB", "ExtN", "FuncDB", "Stats"]
    step_var = tk.StringVar(value="")
    step_label = ttk.Label(frame, textvariable=step_var, style="Status.TLabel", foreground="#8b949e")
    step_label.grid(row=6, column=0, columnspan=2, pady=(0, 2))

    style.configure("green.Horizontal.TProgressbar", troughcolor=bg2, background=green)
    progress = ttk.Progressbar(frame, length=400, mode="determinate",
                                maximum=len(STEPS),
                                style="green.Horizontal.TProgressbar")
    progress.grid(row=7, column=0, columnspan=2, pady=(0, 10))

    # Overall progress
    overall_var = tk.StringVar(value="")
    overall_label = ttk.Label(frame, textvariable=overall_var, style="Status.TLabel", foreground="#8b949e")
    overall_label.grid(row=8, column=0, columnspan=2, pady=(0, 2))

    style.configure("blue.Horizontal.TProgressbar", troughcolor=bg2, background=blue)
    overall_progress = ttk.Progressbar(frame, length=400, mode="determinate",
                                        maximum=100,
                                        style="blue.Horizontal.TProgressbar")
    overall_progress.grid(row=9, column=0, columnspan=2, pady=(0, 10))

    # Log area
    log_text = tk.Text(frame, height=10, width=55, bg=bg2, fg=fg, font=("Consolas", 9),
                       relief="flat", state="disabled", wrap="word")
    log_text.grid(row=10, column=0, columnspan=2, pady=(5, 15))

    def log(msg):
        log_text.configure(state="normal")
        log_text.insert("end", msg + "\n")
        log_text.see("end")
        log_text.configure(state="disabled")

    def _update_overall():
        """Periodically fetch task stats from coordinator and update overall progress."""
        while not stop_event.is_set():
            try:
                import requests
                resp = requests.get(f"{config.COORDINATOR_URL}/api/tasks/stats", timeout=5)
                if resp.ok:
                    data = resp.json()
                    total = data.get("total", 0)
                    completed = data.get("by_status", {}).get("completed", 0)
                    pending = data.get("by_status", {}).get("pending", 0)
                    running = data.get("by_status", {}).get("running", 0)
                    failed = data.get("by_status", {}).get("failed", 0)
                    pct = round(completed / total * 100) if total else 0
                    workers = data.get("active_workers", "?")
                    text = f"Overall: {completed}/{total} done | {running} running | {pending} left | {workers} workers"
                    root.after(0, lambda t=text, p=pct: (
                        overall_var.set(t),
                        overall_progress.configure(value=p),
                    ))
            except Exception:
                pass
            stop_event.wait(30)

    def _watch_log(accession, task_start_time):
        """Tail the log file and update progress bar based on step markers."""
        log_path = config.LOGS_DIR / f"{accession}.log"
        seen_pos = 0
        current_step = 0
        step_times = {}  # step_n -> timestamp when it started
        step_map = {
            "Step 1": (1, "Downloading from SRA..."),
            "Step 2": (2, "Quality trimming..."),
            "NCycDB": (3, "DIAMOND vs NCycDB..."),
            "PlasticDB": (4, "DIAMOND vs PlasticDB..."),
            "ExtN": (5, "DIAMOND vs ExtN..."),
            "extended N": (5, "DIAMOND vs ExtN..."),
            "functional": (6, "DIAMOND vs FuncDB..."),
            "FuncDB": (6, "DIAMOND vs FuncDB..."),
            "Step 7": (7, "Saving stats..."),
            "Pipeline complete": (7, "Done!"),
        }
        import re
        while not stop_event.is_set():
            try:
                if log_path.exists():
                    with open(log_path, "r") as f:
                        f.seek(seen_pos)
                        new = f.read()
                        seen_pos = f.tell()

                    # Check for download progress (e.g. "file.fastq.gz: 45/120 MB (37%)")
                    dl_match = re.findall(r"(\S+\.fastq\.gz): (\d+)/(\d+) MB \((\d+)%\)", new)
                    if dl_match and current_step <= 1:
                        fname, mb, total_mb, pct = dl_match[-1]
                        root.after(0, lambda f=fname, p=pct, m=mb, t=total_mb: (
                            progress.configure(value=int(p) / 100 * len(STEPS)),
                            step_var.set(f"Step 1/{len(STEPS)}: Downloading {f} ({m}/{t} MB)"),
                        ))

                    for marker, (step_n, label) in step_map.items():
                        if marker in new and step_n > current_step:
                            current_step = step_n
                            now = time.time()
                            step_times[step_n] = now

                            # Estimate remaining time
                            elapsed = now - task_start_time
                            if current_step > 0 and current_step < len(STEPS):
                                per_step = elapsed / current_step
                                remaining = per_step * (len(STEPS) - current_step)
                                rm = int(remaining) // 60
                                rs = int(remaining) % 60
                                eta = f" ~{rm}m{rs:02d}s left"
                            elif current_step >= len(STEPS):
                                eta = ""
                            else:
                                eta = ""

                            root.after(0, lambda n=step_n, l=label, e=eta: (
                                progress.configure(value=n),
                                step_var.set(f"Step {n}/{len(STEPS)}: {l}{e}"),
                            ))
            except Exception:
                pass
            stop_event.wait(2)

    # Worker thread
    worker_thread = None
    log_watcher = None
    stop_event = threading.Event()
    pause_event = threading.Event()

    def run_worker():
        # Override thread count
        config.THREADS = thread_var.get()
        os.environ["THREADS"] = str(config.THREADS)

        root.after(0, lambda: status_var.set("Connecting..."))
        root.after(0, lambda: status_label.configure(foreground=blue))

        from . import uploader
        from .executor import run_sample, collect_result_files

        # Register
        try:
            worker_id = uploader.register_worker()
            root.after(0, lambda: log(f"Registered as {worker_id}"))
        except Exception as e:
            root.after(0, lambda: log(f"Failed to register: {e}"))
            root.after(0, lambda: status_var.set("Connection failed"))
            root.after(0, lambda: status_label.configure(foreground=red))
            return

        # Sync databases
        try:
            root.after(0, lambda: status_var.set("Syncing databases..."))
            uploader.sync_databases()
            root.after(0, lambda: log("Databases synced"))
        except Exception as e:
            root.after(0, lambda: log(f"DB sync failed: {e}"))
            root.after(0, lambda: status_var.set("Database sync failed"))
            root.after(0, lambda: status_label.configure(foreground=red))
            return

        root.after(0, lambda: status_var.set("Running"))
        root.after(0, lambda: status_label.configure(foreground=green))

        while not stop_event.is_set():
            # Heartbeat
            enabled = uploader.send_heartbeat(worker_id)
            if not enabled:
                root.after(0, lambda: status_var.set("Paused by coordinator"))
                root.after(0, lambda: status_label.configure(foreground="#fab387"))
                stop_event.wait(config.POLL_INTERVAL_S)
                continue

            # Check DB updates
            try:
                if uploader.check_db_updates():
                    root.after(0, lambda: log("Database update — re-syncing..."))
                    uploader.sync_databases()
            except Exception:
                pass

            # Don't claim new tasks while paused
            while pause_event.is_set() and not stop_event.is_set():
                stop_event.wait(config.POLL_INTERVAL_S)
            if stop_event.is_set():
                break

            # Claim
            try:
                task = uploader.claim_task(worker_id)
            except Exception as e:
                root.after(0, lambda: log(f"Claim failed: {e}"))
                stop_event.wait(config.POLL_INTERVAL_S)
                continue

            if not task:
                root.after(0, lambda: status_var.set("Waiting for tasks..."))
                stop_event.wait(config.POLL_INTERVAL_S)
                root.after(0, lambda: status_var.set("Running"))
                continue

            acc = task["accession"]
            study = task["study"]
            root.after(0, lambda a=acc, s=study: status_var.set(f"Processing {a} ({s})"))
            root.after(0, lambda a=acc, s=study: log(f"Started {a} ({s})"))
            root.after(0, lambda: (progress.configure(value=0), step_var.set("")))

            # Start log watcher for progress
            task_start = time.time()
            log_watcher = threading.Thread(target=_watch_log, args=(acc, task_start), daemon=True)
            log_watcher.start()

            success, duration_s, error, step_timings = run_sample(acc)

            if success:
                dm, ds = duration_s // 60, duration_s % 60
                root.after(0, lambda a=acc, dm=dm, ds=ds: log(f"Completed {a} ({dm}m {ds}s)"))

                files = collect_result_files(acc)
                if files:
                    try:
                        uploader.upload_results(acc, files)
                    except Exception:
                        pass
                try:
                    uploader.complete_task(acc, worker_id, duration_s, step_timings)
                except Exception:
                    pass
            else:
                root.after(0, lambda a=acc, e=error[:100]: log(f"FAILED {a}: {e}"))
                try:
                    uploader.fail_task(acc, worker_id, error)
                except Exception:
                    pass

        root.after(0, lambda: status_var.set("Stopped"))
        root.after(0, lambda: status_label.configure(foreground=fg))

    def start():
        nonlocal worker_thread
        stop_event.clear()
        pause_event.clear()
        start_btn.configure(state="disabled")
        pause_btn.configure(state="normal")
        stop_btn.configure(state="normal")
        scale.configure(state="disabled")
        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()
        threading.Thread(target=_update_overall, daemon=True).start()

    def pause():
        from .executor import suspend_current, resume_current
        if pause_event.is_set():
            pause_event.clear()
            resume_current()
            pause_btn.configure(text="Pause", bg="#fab387")
            status_label.configure(foreground=green)
            status_var.set("Running")
            log("Resumed")
        else:
            pause_event.set()
            suspend_current()
            pause_btn.configure(text="Resume", bg=blue)
            status_label.configure(foreground="#fab387")
            status_var.set("Paused")
            log("Paused")

    def stop():
        from .executor import resume_current
        if pause_event.is_set():
            resume_current()  # unfreeze so it can finish
        stop_event.set()
        pause_event.clear()
        start_btn.configure(state="normal")
        pause_btn.configure(state="disabled", text="Pause", bg="#fab387")
        stop_btn.configure(state="disabled")
        scale.configure(state="normal")
        log("Stopping after current task...")

    # Buttons
    btn_frame = ttk.Frame(frame)
    btn_frame.grid(row=11, column=0, columnspan=2)

    start_btn = tk.Button(btn_frame, text="Start", command=start, bg=green, fg="#1e1e2e",
                          font=("Segoe UI", 12, "bold"), width=8, relief="flat", cursor="hand2")
    start_btn.pack(side="left", padx=5)

    pause_btn = tk.Button(btn_frame, text="Pause", command=pause, bg="#fab387", fg="#1e1e2e",
                          font=("Segoe UI", 12, "bold"), width=8, relief="flat", cursor="hand2",
                          state="disabled")
    pause_btn.pack(side="left", padx=5)

    stop_btn = tk.Button(btn_frame, text="Stop", command=stop, bg=red, fg="#1e1e2e",
                         font=("Segoe UI", 12, "bold"), width=8, relief="flat", cursor="hand2",
                         state="disabled")
    stop_btn.pack(side="left", padx=5)

    root.protocol("WM_DELETE_WINDOW", lambda: (stop(), root.after(1000, root.destroy)))
    root.mainloop()


if __name__ == "__main__":
    main()
