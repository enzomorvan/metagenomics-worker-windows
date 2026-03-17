import os
import platform
import signal
import shutil
import subprocess
import time
from pathlib import Path

from . import config

WRAPPER_SCRIPT = Path(__file__).resolve().parent.parent / "process_sample_wrapper.sh"
IS_NATIVE_WINDOWS = platform.system() == "Windows" and "microsoft" not in platform.release().lower()

# Handle to the currently running subprocess (for pause/resume)
_current_proc: subprocess.Popen | None = None


def suspend_current():
    """Suspend the running pipeline process."""
    if _current_proc and _current_proc.poll() is None:
        try:
            if IS_NATIVE_WINDOWS:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1F0FFF, False, _current_proc.pid)
                kernel32.DebugActiveProcess(_current_proc.pid)
            else:
                os.kill(_current_proc.pid, signal.SIGSTOP)
        except Exception:
            pass


def resume_current():
    """Resume the suspended pipeline process."""
    if _current_proc and _current_proc.poll() is None:
        try:
            if IS_NATIVE_WINDOWS:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.DebugActiveProcessStop(_current_proc.pid)
            else:
                os.kill(_current_proc.pid, signal.SIGCONT)
        except Exception:
            pass


def _detect_ram_gb() -> float:
    try:
        import psutil
        return psutil.virtual_memory().total / (1024**3)
    except ImportError:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    return int(line.split()[1]) / (1024**2)
    except Exception:
        pass
    return 8.0


def _diamond_settings() -> tuple[str, str]:
    # Our databases are small (~240 MB total), so always use index-chunks 1
    # to load the full index into RAM once. This avoids repeated disk reads
    # which are catastrophically slow on HDDs.
    return "2.0", "1"


def _find_tool(name: str) -> str:
    """Find a tool binary, checking PATH."""
    ext = ".exe" if IS_NATIVE_WINDOWS else ""
    found = shutil.which(name + ext) or shutil.which(name)
    if found:
        return found
    return name + ext


def run_sample(accession: str) -> tuple[bool, int, str, dict | None]:
    """Run the metagenomics pipeline for a single accession.

    Uses bash wrapper on Linux/WSL, native Python orchestration on Windows.
    Returns (success, duration_seconds, error_message, step_timings).
    """
    config.WORK_DIR.mkdir(parents=True, exist_ok=True)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)

    if IS_NATIVE_WINDOWS:
        return _run_native_windows(accession)
    else:
        return _run_bash(accession)


def _parse_step_timings(log_file: Path) -> dict | None:
    """Parse STEP_TIMINGS line from bash wrapper log."""
    try:
        for line in log_file.read_text().splitlines():
            if line.startswith("STEP_TIMINGS:"):
                parts = line[len("STEP_TIMINGS:"):].split(",")
                return {kv.split("=")[0]: int(kv.split("=")[1]) for kv in parts}
    except Exception:
        pass
    return None


def _run_bash(accession: str) -> tuple[bool, int, str, dict | None]:
    """Run via bash wrapper (Linux/WSL/macOS)."""
    global _current_proc
    log_file = config.LOGS_DIR / f"{accession}.log"
    block_size, index_chunks = _diamond_settings()

    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/usr/local/bin"),
        "HOME": str(Path.home()),
        "ACCESSION": accession,
        "DB_DIR": str(config.DB_DIR),
        "RESULTS_DIR": str(config.RESULTS_DIR),
        "WORK_DIR": str(config.WORK_DIR / f"tmp_{accession}"),
        "THREADS": str(config.THREADS),
        "BLOCK_SIZE": block_size,
        "INDEX_CHUNKS": index_chunks,
    }

    start = time.time()
    try:
        with open(log_file, "w") as lf:
            _current_proc = subprocess.Popen(
                ["bash", str(WRAPPER_SCRIPT), accession],
                env=env, stdout=lf, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                _current_proc.wait(timeout=7200)
            except subprocess.TimeoutExpired:
                _current_proc.kill()
                _current_proc.wait()
                _current_proc = None
                return False, int(time.time() - start), "Timeout: exceeded 2 hour limit", None

        returncode = _current_proc.returncode
        _current_proc = None
        duration = int(time.time() - start)
        timings = _parse_step_timings(log_file)
        if returncode == 0:
            return True, duration, "", timings
        try:
            error = "\n".join(log_file.read_text().splitlines()[-20:])
        except Exception:
            error = f"Exit code {returncode}"
        return False, duration, error, timings
    except Exception as e:
        _current_proc = None
        return False, int(time.time() - start), str(e), None


def _run_native_windows(accession: str) -> tuple[bool, int, str, dict | None]:
    """Run pipeline natively on Windows using .exe tools."""
    log_file = config.LOGS_DIR / f"{accession}.log"
    work = config.WORK_DIR / f"tmp_{accession}"
    work.mkdir(parents=True, exist_ok=True)
    results = config.RESULTS_DIR
    db = config.DB_DIR
    threads = str(config.THREADS)
    block_size, index_chunks = _diamond_settings()

    EVALUE, IDENTITY, QUERY_COV = "1e-10", "50", "50"
    SUBSAMPLE_READS = 2500000

    fastp_bin = _find_tool("fastp")
    diamond = _find_tool("diamond")

    sra_env = os.environ.copy()

    start = time.time()
    try:
        with open(log_file, "w") as log:
            def run(cmd, **kw):
                global _current_proc
                log.write(f">>> {' '.join(str(c) for c in cmd)}\n")
                log.flush()
                _current_proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                                 env=sra_env, **kw)
                ret = _current_proc.wait(timeout=7200)
                _current_proc = None
                if ret != 0:
                    raise subprocess.CalledProcessError(ret, cmd)

            _timings = {}

            # Step 1: Download FASTQ from ENA (direct HTTP, no SRA toolkit needed)
            log.write(f"=== [{accession}] Step 1: Downloading from ENA ===\n")
            _t = time.time()
            r1, r2, se, paired = _download_from_ena(accession, work, log)
            _timings["download"] = int(time.time() - _t)
            log.write(f"  Download time: {_timings['download']}s\n")

            # Step 1.5: Subsample before QC (avoid processing entire file)
            log.write(f"  Subsampling to {SUBSAMPLE_READS} reads per mate before QC...\n")
            log.flush()
            if paired:
                r1_fq = work / f"{accession}_1.fastq"
                r2_fq = work / f"{accession}_2.fastq"
                _subsample_gz(r1, r1_fq, SUBSAMPLE_READS)
                _subsample_gz(r2, r2_fq, SUBSAMPLE_READS)
                r1, r2 = r1_fq, r2_fq
            else:
                se_fq = work / f"{accession}.fastq"
                _subsample_gz(se, se_fq, SUBSAMPLE_READS * 2)
                se = se_fq
            log.write(f"  Subsample done.\n")

            # Step 2: QC
            _t = time.time()
            trimmed = work / f"{accession}_trimmed.fastq"
            has_fastp = shutil.which(fastp_bin) is not None

            if has_fastp:
                log.write(f"\n=== [{accession}] Step 2: Quality trimming with fastp ===\n")
                if paired:
                    t1 = work / f"{accession}_trimmed_1.fastq"
                    t2 = work / f"{accession}_trimmed_2.fastq"
                    run([fastp_bin, "--in1", str(r1), "--in2", str(r2),
                         "--out1", str(t1), "--out2", str(t2),
                         "--qualified_quality_phred", "20", "--length_required", "50",
                         "--thread", threads,
                         "--json", str(work / f"{accession}_fastp.json"),
                         "--html", os.devnull])
                    r1.unlink(missing_ok=True)
                    r2.unlink(missing_ok=True)
                    sub_lines = SUBSAMPLE_READS * 4
                    _head_concat(t1, t2, trimmed, sub_lines)
                    t1.unlink(missing_ok=True)
                    t2.unlink(missing_ok=True)
                else:
                    run([fastp_bin, "--in1", str(se), "--out1", str(trimmed),
                         "--qualified_quality_phred", "20", "--length_required", "50",
                         "--thread", threads,
                         "--json", str(work / f"{accession}_fastp.json"),
                         "--html", os.devnull])
                    se.unlink(missing_ok=True)
                    sub_lines = SUBSAMPLE_READS * 2 * 4
                    _head_inplace(trimmed, sub_lines)
            else:
                # Use cutadapt as fallback (available on Windows via pip)
                log.write(f"\n=== [{accession}] Step 2: Quality trimming with cutadapt ===\n")
                cutadapt_bin = shutil.which("cutadapt") or shutil.which("cutadapt.exe")
                if cutadapt_bin is None:
                    # Try as Python module
                    cutadapt_bin = None
                    try:
                        import cutadapt as _
                        cutadapt_bin = "module"
                    except ImportError:
                        pass

                if cutadapt_bin == "module":
                    # Run as python -m cutadapt
                    import sys
                    cutadapt_cmd = [sys.executable, "-m", "cutadapt"]
                elif cutadapt_bin:
                    cutadapt_cmd = [cutadapt_bin]
                else:
                    cutadapt_cmd = None

                if cutadapt_cmd:
                    cut_threads = str(max(1, int((os.cpu_count() or 4) * 0.75)))
                    if paired:
                        t1 = work / f"{accession}_trimmed_1.fastq"
                        t2 = work / f"{accession}_trimmed_2.fastq"
                        run(cutadapt_cmd + [
                            "-j", cut_threads,
                            "-a", "AGATCGGAAGAGC", "-A", "AGATCGGAAGAGC",
                            "-q", "20", "-m", "50",
                            "-o", str(t1), "-p", str(t2),
                            str(r1), str(r2)])
                        r1.unlink(missing_ok=True)
                        r2.unlink(missing_ok=True)
                        sub_lines = SUBSAMPLE_READS * 4
                        _head_concat(t1, t2, trimmed, sub_lines)
                        t1.unlink(missing_ok=True)
                        t2.unlink(missing_ok=True)
                    else:
                        run(cutadapt_cmd + [
                            "-j", cut_threads,
                            "-a", "AGATCGGAAGAGC",
                            "-q", "20", "-m", "50",
                            "-o", str(trimmed), str(se)])
                        se.unlink(missing_ok=True)
                        sub_lines = SUBSAMPLE_READS * 2 * 4
                        _head_inplace(trimmed, sub_lines)
                else:
                    # Last resort: decompress and subsample raw reads
                    import gzip as _gzip
                    log.write("  WARNING: No QC tool available, decompressing and subsampling raw reads\n")
                    if paired:
                        r1_fq, r2_fq = work / f"{accession}_1.fastq", work / f"{accession}_2.fastq"
                        for gz, fq in [(r1, r1_fq), (r2, r2_fq)]:
                            with _gzip.open(gz, "rb") as gi, open(fq, "wb") as fo:
                                while True:
                                    chunk = gi.read(4 * 1024 * 1024)
                                    if not chunk:
                                        break
                                    fo.write(chunk)
                            gz.unlink()
                        sub_lines = SUBSAMPLE_READS * 4
                        _head_concat(r1_fq, r2_fq, trimmed, sub_lines)
                        r1_fq.unlink(missing_ok=True)
                        r2_fq.unlink(missing_ok=True)
                    else:
                        se_fq = work / f"{accession}.fastq"
                        with _gzip.open(se, "rb") as gi, open(se_fq, "wb") as fo:
                            while True:
                                chunk = gi.read(4 * 1024 * 1024)
                                if not chunk:
                                    break
                                fo.write(chunk)
                        se.unlink()
                        sub_lines = SUBSAMPLE_READS * 2 * 4
                        _head_inplace(se_fq, sub_lines)
                        se_fq.rename(trimmed)

            total_reads = sum(1 for _ in open(trimmed, "r")) // 4
            _timings["qc"] = int(time.time() - _t)
            log.write(f"  Reads after trimming: {total_reads}\n")
            layout = "paired" if paired else "single"

            # Step 3-6: DIAMOND searches
            dbs = [
                ("NCycDB",    "ncycdb/NCyc_100.dmnd",                        "ncycdb/id2gene.map",                        "ncyc"),
                ("PlasticDB", "expanded_plasticdb/expanded_plasticdb.dmnd",   "expanded_plasticdb/expanded_plasticdb_annotations.tsv", "plastic"),
                ("ExtN",      "extended_ndb/extended_ndb.dmnd",               "extended_ndb/extended_ndb_id2gene.map",      "extN"),
                ("FuncDB",    "functional_db/functional_db.dmnd",             "functional_db/functional_db_id2gene.map",    "func"),
            ]

            hit_totals = {}
            for name, db_rel, map_rel, prefix in dbs:
                db_path = db / db_rel
                map_path = db / map_rel
                hits_file = work / f"{accession}_{prefix}_hits.tsv"
                counts_file = results / f"{accession}_{prefix}_counts.tsv"

                if not db_path.exists():
                    log.write(f"\n  WARNING: {name} not found — skipping.\n")
                    counts_file.write_text("gene_family\thit_count\n")
                    hit_totals[prefix] = 0
                    continue

                log.write(f"\n=== [{accession}] DIAMOND blastx vs {name} ===\n")
                _t = time.time()
                run([diamond, "blastx",
                     "--query", str(trimmed), "--db", str(db_path),
                     "--out", str(hits_file),
                     "--evalue", EVALUE, "--id", IDENTITY,
                     "--query-cover", QUERY_COV, "--max-target-seqs", "1",
                     "--outfmt", "6", "--threads", threads,
                     "--block-size", block_size, "--index-chunks", index_chunks])

                # Count hits
                total = _count_hits(hits_file, map_path, counts_file, prefix, db)
                hit_totals[prefix] = total
                _timings[prefix] = int(time.time() - _t)
                log.write(f"  {name} total hits: {total}\n")

            # Step 7: Stats
            stats_file = results / f"{accession}_stats.tsv"
            with open(stats_file, "w") as sf:
                sf.write("accession\tlayout\ttotal_reads_trimmed\tncyc_hits\tplastic_hits\textN_hits\tfunc_hits\n")
                sf.write(f"{accession}\t{layout}\t{total_reads}\t{hit_totals.get('ncyc',0)}\t{hit_totals.get('plastic',0)}\t{hit_totals.get('extN',0)}\t{hit_totals.get('func',0)}\n")

            log.write(f"\n=== [{accession}] Pipeline complete ===\n")

        # Cleanup
        shutil.rmtree(work, ignore_errors=True)

        duration = int(time.time() - start)
        return True, duration, "", _timings

    except subprocess.TimeoutExpired:
        shutil.rmtree(work, ignore_errors=True)
        return False, int(time.time() - start), "Timeout: exceeded 2 hour limit", None
    except Exception as e:
        shutil.rmtree(work, ignore_errors=True)
        try:
            error = "\n".join(log_file.read_text().splitlines()[-20:])
        except Exception:
            error = str(e)
        return False, int(time.time() - start), error, None


def _download_from_ena(accession: str, work: Path, log) -> tuple[Path, Path, Path, bool]:
    """Download FASTQ files directly from ENA via HTTP.

    Returns (r1, r2, se, paired). For paired-end, r1 and r2 are set.
    For single-end, se is set.
    """
    import gzip
    import requests

    # Query ENA API for FASTQ URLs
    log.write(f"  Querying ENA for {accession}...\n")
    log.flush()
    resp = requests.get(
        f"https://www.ebi.ac.uk/ena/portal/api/filereport"
        f"?accession={accession}&result=read_run&fields=fastq_ftp",
        timeout=30,
    )
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    if len(lines) < 2 or not lines[1].strip():
        raise RuntimeError(f"ENA has no FASTQ files for {accession}")

    ftp_field = lines[1].split("\t")[1]
    urls = [u.strip() for u in ftp_field.split(";") if u.strip()]

    # Download each file (keep as .gz — cutadapt and DIAMOND read gzip natively)
    for url in urls:
        http_url = "https://" + url
        filename = url.split("/")[-1]
        gz_path = work / filename
        log.write(f"  Downloading {filename}...\n")
        log.flush()

        with requests.get(http_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded = 0
            with open(gz_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        pct = downloaded * 100 // total_size
                        mb = downloaded // (1024 * 1024)
                        total_mb = total_size // (1024 * 1024)
                        log.write(f"\r  {filename}: {mb}/{total_mb} MB ({pct}%)")
                        log.flush()
            if total_size > 0:
                log.write("\n")

    r1 = work / f"{accession}_1.fastq.gz"
    r2 = work / f"{accession}_2.fastq.gz"
    se = work / f"{accession}.fastq.gz"

    paired = r1.exists() and r2.exists()
    if not paired and not se.exists():
        for f in work.glob("*.fastq.gz"):
            if f.name != r1.name and f.name != r2.name:
                f.rename(se)
                break

    if not paired and not se.exists():
        raise RuntimeError(f"No FASTQ files produced for {accession}")

    layout = "paired-end" if paired else "single-end"
    log.write(f"  Layout: {layout}\n")
    return r1, r2, se, paired


def _subsample_gz(gz_path: Path, out_path: Path, max_reads: int):
    """Decompress .gz and keep only the first max_reads FASTQ records."""
    import gzip
    max_lines = max_reads * 4
    with gzip.open(gz_path, "rt") as gi, open(out_path, "w") as fo:
        for i, line in enumerate(gi):
            if i >= max_lines:
                break
            fo.write(line)
    gz_path.unlink()


def _head_concat(f1: Path, f2: Path, out: Path, max_lines: int):
    """Subsample and concatenate two FASTQ files."""
    with open(out, "w") as o:
        for src in [f1, f2]:
            with open(src) as f:
                for i, line in enumerate(f):
                    if i >= max_lines:
                        break
                    o.write(line)


def _head_inplace(f: Path, max_lines: int):
    """Truncate a file to max_lines."""
    tmp = f.with_suffix(".sub")
    with open(f) as src, open(tmp, "w") as dst:
        for i, line in enumerate(src):
            if i >= max_lines:
                break
            dst.write(line)
    tmp.replace(f)


def _count_hits(hits_file: Path, map_path: Path, counts_file: Path,
                prefix: str, db_dir: Path) -> int:
    """Count hits per gene family from DIAMOND output. Returns total hits."""
    if not hits_file.exists() or hits_file.stat().st_size == 0:
        counts_file.write_text("gene_family\thit_count\n")
        return 0

    # Load mapping
    gene_map = {}
    if map_path.exists():
        with open(map_path) as f:
            for line in f:
                parts = line.strip().split("\t", 1)
                if len(parts) == 2:
                    gene_map[parts[0]] = parts[1]

    # For NCycDB, also parse FASTA headers for m5nr entries
    if prefix == "ncyc":
        faa = db_dir / "ncycdb" / "NCyc_100.faa"
        if faa.exists():
            with open(faa) as f:
                for line in f:
                    if line.startswith(">"):
                        parts = line[1:].split()
                        seq_id = parts[0]
                        for p in parts[1:]:
                            if p.startswith("[description="):
                                gene = p.replace("[description=", "").rstrip("]")
                                if gene:
                                    gene_map[seq_id] = gene
                                break

    # Count
    counts = {}
    with open(hits_file) as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) < 2:
                continue
            sid = cols[1]
            # Try sp|ACC|NAME format
            sp_parts = sid.split("|")
            acc = sp_parts[1] if len(sp_parts) >= 2 and sp_parts[1] else sid
            gene = gene_map.get(acc) or gene_map.get(sid) or "unknown"
            counts[gene] = counts.get(gene, 0) + 1

    # Write
    header = "enzyme_id\thit_count" if prefix == "plastic" else "gene_family\thit_count"
    with open(counts_file, "w") as f:
        f.write(header + "\n")
        for gene, count in sorted(counts.items(), key=lambda x: -x[1]):
            f.write(f"{gene}\t{count}\n")

    # Plastic: also write by-target counts
    if prefix == "plastic":
        annot_path = db_dir / "expanded_plasticdb" / "expanded_plasticdb_annotations.tsv"
        by_target_file = counts_file.parent / f"{hits_file.stem.replace('_plastic_hits', '')}_plastic_by_target.tsv"
        _count_plastic_by_target(hits_file, annot_path, by_target_file)

    return sum(counts.values())


def _count_plastic_by_target(hits_file: Path, annot_path: Path, out_file: Path):
    """Count plastic hits by target plastic type."""
    target_map = {}
    if annot_path.exists():
        with open(annot_path) as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 4 and parts[0] != "seq_id":
                    target_map[parts[0]] = parts[3]

    counts = {}
    with open(hits_file) as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) < 2:
                continue
            target = target_map.get(cols[1], "unmapped")
            counts[target] = counts.get(target, 0) + 1

    with open(out_file, "w") as f:
        f.write("target_plastic\thit_count\n")
        for target, count in sorted(counts.items(), key=lambda x: -x[1]):
            f.write(f"{target}\t{count}\n")


def collect_result_files(accession: str) -> list[Path]:
    """Collect the output TSV files for an accession."""
    results = []
    for pattern in [
        f"{accession}_stats.tsv",
        f"{accession}_ncyc_counts.tsv",
        f"{accession}_plastic_counts.tsv",
        f"{accession}_plastic_by_target.tsv",
        f"{accession}_extN_counts.tsv",
        f"{accession}_func_counts.tsv",
    ]:
        f = config.RESULTS_DIR / pattern
        if f.exists():
            results.append(f)
    return results
