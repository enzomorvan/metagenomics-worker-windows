# Metagenomics Distributed Worker (Windows)

Distributed compute worker for metagenomics sample processing. Downloads metagenomic samples from ENA, performs quality trimming (cutadapt), runs DIAMOND blastx against 4 databases (NCycDB, PlasticDB, Extended N-metabolism, Functional genes), and uploads results to a central coordinator.

## Quick Start

1. Extract this folder to a path **without spaces** (e.g. `D:\metagenomics_worker`)
2. Double-click `SETUP.bat`
3. Wait ~10 minutes (downloads Python, DIAMOND, cutadapt)
4. GUI opens — pick thread count, click **Start**

Next time: double-click `Start_Worker.bat`

## Requirements

- Windows 10/11
- Internet connection
- ~2 GB disk for tools, plus ~30 GB free for processing

## What Gets Installed

Everything installs into this folder:

| Tool | Purpose | Source |
|------|---------|--------|
| Miniforge (Python) | Worker runtime + cutadapt | conda-forge |
| DIAMOND | Protein alignment (blastx) | GitHub |
| cutadapt | Quality trimming + adapter removal | pip |

## GUI Features

- Thread count selector (1 to CPU cores)
- Start / Pause / Stop buttons
- Per-sample progress bar with download progress and time estimate
- Overall progress (all workers combined)
- CPU temperature monitoring

## Pipeline Per Sample

1. Download FASTQ from ENA (European Nucleotide Archive)
2. Quality trim with cutadapt (Q20, min 50bp, Illumina adapter removal)
3. Subsample to 5M reads
4. DIAMOND blastx vs NCycDB (nitrogen cycling genes)
5. DIAMOND blastx vs PlasticDB (plastic degradation enzymes)
6. DIAMOND blastx vs Extended N-metabolism DB
7. DIAMOND blastx vs Functional gene DB
8. Count hits per gene family, upload results

## Coordinator

Results are sent to a central coordinator server. The coordinator URL and API key are pre-configured in the worker.

## Troubleshooting

- **"This folder has spaces in its path"** — Move the folder to a path without spaces (e.g. `D:\metagenomics_worker`)
- **Worker won't start** — Make sure SETUP.bat completed successfully. Re-run it if needed.
- **Download failures** — Check internet connection. Failed tasks auto-retry after 30 minutes.
