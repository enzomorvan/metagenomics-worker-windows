#!/bin/bash
# =============================================================================
# process_sample_wrapper.sh
# Thin wrapper around the original process_sample.sh logic.
# Accepts paths via environment variables so it can run on any machine.
#
# Required env vars:
#   ACCESSION   - SRA accession (e.g., SRR3401482)
#   DB_DIR      - Path to DIAMOND databases directory
#   RESULTS_DIR - Path to store result TSV files
#   WORK_DIR    - Temporary working directory for this sample
#   THREADS     - Number of threads for DIAMOND/fastp (default: 12)
# =============================================================================
set -euo pipefail

# ---- argument / env parsing -------------------------------------------------
ACCESSION="${ACCESSION:-${1:-}}"
if [[ -z "${ACCESSION}" ]]; then
    echo "Usage: ACCESSION=SRR12345 bash $0  (or: bash $0 SRR12345)"
    exit 1
fi

DB_DIR="${DB_DIR:?DB_DIR must be set}"
RESULTS_DIR="${RESULTS_DIR:?RESULTS_DIR must be set}"
WORK_DIR="${WORK_DIR:-/tmp/metagenomics_tmp_${ACCESSION}}"
THREADS="${THREADS:-12}"

# Subsampling: 2.5M reads per mate (5M total)
SUBSAMPLE_READS=2500000
SUBSAMPLE_LINES=$(( SUBSAMPLE_READS * 4 ))

# DIAMOND parameters
EVALUE="1e-10"
IDENTITY=50
QUERY_COV=50
MAX_TARGETS=1
# Memory management: override via env vars for low-RAM workers (e.g., VPS with 4GB)
# block-size 0.5 + index-chunks 4 keeps peak RAM under ~2 GB
BLOCK_SIZE="${BLOCK_SIZE:-2.0}"
INDEX_CHUNKS="${INDEX_CHUNKS:-1}"
OUTFMT="6"

# Database paths
NCYC_DB="${DB_DIR}/ncycdb/NCyc_100.dmnd"
NCYC_MAP="${DB_DIR}/ncycdb/id2gene.map"
NCYC_FAA="${DB_DIR}/ncycdb/NCyc_100.faa"
PLASTIC_DB="${DB_DIR}/expanded_plasticdb/expanded_plasticdb.dmnd"
PLASTIC_ANNOT="${DB_DIR}/expanded_plasticdb/expanded_plasticdb_annotations.tsv"
EXTN_DB="${DB_DIR}/extended_ndb/extended_ndb.dmnd"
EXTN_MAP="${DB_DIR}/extended_ndb/extended_ndb_id2gene.map"
FUNC_DB="${DB_DIR}/functional_db/functional_db.dmnd"
FUNC_MAP="${DB_DIR}/functional_db/functional_db_id2gene.map"

# ---- dependency check -------------------------------------------------------
echo "=== [${ACCESSION}] Checking dependencies ==="
MISSING=0
for tool in prefetch fasterq-dump fastp diamond; do
    if ! command -v "$tool" &>/dev/null; then
        echo "ERROR: $tool not found in PATH."
        MISSING=1
    fi
done
if [[ ${MISSING} -eq 1 ]]; then
    echo "Please install missing tools before running this pipeline."
    exit 1
fi

if [[ ! -f "${NCYC_DB}" ]] && [[ ! -f "${PLASTIC_DB}" ]]; then
    echo "ERROR: No DIAMOND databases found in ${DB_DIR}."
    exit 1
fi

# ---- create directories -----------------------------------------------------
mkdir -p "${RESULTS_DIR}" "${WORK_DIR}"

cleanup() {
    echo "=== [${ACCESSION}] Cleaning up temporary files ==="
    rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# =============================================================================
# STEP 1: Download raw reads
# =============================================================================
echo ""
STEP1_START=$(date +%s)
echo "=== [${ACCESSION}] Step 1: Downloading reads ==="

# Try SRA toolkit first, fall back to ENA direct download if it fails
SRA_OK=0
if command -v prefetch &>/dev/null && command -v fasterq-dump &>/dev/null; then
    echo "  Trying SRA toolkit..."
    if prefetch "${ACCESSION}" --output-directory "${WORK_DIR}" --max-size 50G 2>&1 | tail -5; then
        SRA_FILE="${WORK_DIR}/${ACCESSION}/${ACCESSION}.sra"
        # Check there's enough disk for conversion (need ~3x the .sra file size)
        if [[ -f "${SRA_FILE}" ]]; then
            SRA_SIZE=$(stat -c%s "${SRA_FILE}" 2>/dev/null || stat -f%z "${SRA_FILE}" 2>/dev/null)
            AVAIL=$(df --output=avail "${WORK_DIR}" 2>/dev/null | tail -1)
            AVAIL_BYTES=$((AVAIL * 1024))
            NEEDED=$((SRA_SIZE * 3))
            if [[ ${AVAIL_BYTES} -gt ${NEEDED} ]]; then
                if fasterq-dump "${SRA_FILE}" --outdir "${WORK_DIR}" --split-3 --skip-technical --threads "${THREADS}" 2>&1 | tail -5; then
                    SRA_OK=1
                fi
            else
                echo "  Not enough disk for fasterq-dump (need ~$((NEEDED/1024/1024/1024))GB), falling back to ENA..."
            fi
        fi
        rm -rf "${WORK_DIR}/${ACCESSION}"
    fi
fi

# Fallback: stream FASTQ directly from ENA (downloads only what we need)
if [[ ${SRA_OK} -eq 0 ]]; then
    echo "  Streaming from ENA (only first ${SUBSAMPLE_READS} reads per mate)..."
    ENA_RESP=$(curl -sf "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=${ACCESSION}&result=read_run&fields=fastq_ftp" 2>/dev/null)
    if [[ -z "${ENA_RESP}" ]]; then
        echo "ERROR: Could not fetch ENA metadata for ${ACCESSION}"
        exit 1
    fi
    ENA_URLS=$(echo "${ENA_RESP}" | tail -1 | cut -f2 | tr ';' ' ')
    # Download extra reads to account for fastp filtering (~20% loss)
    STREAM_LINES=$(( SUBSAMPLE_READS * 4 * 2 ))
    for URL in ${ENA_URLS}; do
        FNAME=$(basename "${URL}" .gz)
        echo "  Streaming ${FNAME}..."
        curl -sf "https://${URL}" | gunzip -c | head -n ${STREAM_LINES} > "${WORK_DIR}/${FNAME}" || true
        FLINES=$(wc -l < "${WORK_DIR}/${FNAME}")
        if [[ ${FLINES} -lt 4 ]]; then
            echo "  ERROR: ${FNAME} is empty or failed to download"
            exit 1
        fi
        # Truncate to complete FASTQ records (multiple of 4 lines)
        FLINES_CLEAN=$(( (FLINES / 4) * 4 ))
        if [[ ${FLINES_CLEAN} -lt ${FLINES} ]]; then
            head -n ${FLINES_CLEAN} "${WORK_DIR}/${FNAME}" > "${WORK_DIR}/${FNAME}.tmp"
            mv "${WORK_DIR}/${FNAME}.tmp" "${WORK_DIR}/${FNAME}"
        fi
        echo "  Got $(( FLINES_CLEAN / 4 )) reads"
    done

    # For paired-end: truncate both mates to the same read count
    if [[ -f "${WORK_DIR}/${ACCESSION}_1.fastq" ]] && [[ -f "${WORK_DIR}/${ACCESSION}_2.fastq" ]]; then
        R1_LINES=$(wc -l < "${WORK_DIR}/${ACCESSION}_1.fastq")
        R2_LINES=$(wc -l < "${WORK_DIR}/${ACCESSION}_2.fastq")
        if [[ ${R1_LINES} -ne ${R2_LINES} ]]; then
            MIN_LINES=$(( R1_LINES < R2_LINES ? R1_LINES : R2_LINES ))
            MIN_LINES=$(( (MIN_LINES / 4) * 4 ))
            echo "  Equalizing mates to $(( MIN_LINES / 4 )) reads each"
            head -n ${MIN_LINES} "${WORK_DIR}/${ACCESSION}_1.fastq" > "${WORK_DIR}/${ACCESSION}_1.fastq.tmp"
            mv "${WORK_DIR}/${ACCESSION}_1.fastq.tmp" "${WORK_DIR}/${ACCESSION}_1.fastq"
            head -n ${MIN_LINES} "${WORK_DIR}/${ACCESSION}_2.fastq" > "${WORK_DIR}/${ACCESSION}_2.fastq.tmp"
            mv "${WORK_DIR}/${ACCESSION}_2.fastq.tmp" "${WORK_DIR}/${ACCESSION}_2.fastq"
        fi
    fi
fi

STEP1_END=$(date +%s)
echo "  Download time: $(( STEP1_END - STEP1_START ))s"

if [[ -f "${WORK_DIR}/${ACCESSION}_1.fastq" ]] && [[ -f "${WORK_DIR}/${ACCESSION}_2.fastq" ]]; then
    LAYOUT="paired"
    echo "  Layout: paired-end"
else
    LAYOUT="single"
    if [[ -f "${WORK_DIR}/${ACCESSION}.fastq" ]]; then
        echo "  Layout: single-end"
    else
        echo "ERROR: No FASTQ files produced for ${ACCESSION}"
        exit 1
    fi
fi

# =============================================================================
# STEP 2: Quality control with fastp
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 2: Quality trimming with fastp ==="

TRIMMED="${WORK_DIR}/${ACCESSION}_trimmed.fastq"

if [[ "${LAYOUT}" == "paired" ]]; then
    fastp \
        --in1 "${WORK_DIR}/${ACCESSION}_1.fastq" \
        --in2 "${WORK_DIR}/${ACCESSION}_2.fastq" \
        --out1 "${WORK_DIR}/${ACCESSION}_trimmed_1.fastq" \
        --out2 "${WORK_DIR}/${ACCESSION}_trimmed_2.fastq" \
        --qualified_quality_phred 20 \
        --length_required 50 \
        --thread "${THREADS}" \
        --json "${WORK_DIR}/${ACCESSION}_fastp.json" \
        --html /dev/null \
        2>&1 | tail -5

    rm -f "${WORK_DIR}/${ACCESSION}_1.fastq" "${WORK_DIR}/${ACCESSION}_2.fastq"

    if [[ ${SUBSAMPLE_READS} -gt 0 ]]; then
        echo "  Subsampling to ${SUBSAMPLE_READS} reads per mate..."
        head -n "${SUBSAMPLE_LINES}" "${WORK_DIR}/${ACCESSION}_trimmed_1.fastq" > "${WORK_DIR}/sub_1.fastq"
        head -n "${SUBSAMPLE_LINES}" "${WORK_DIR}/${ACCESSION}_trimmed_2.fastq" > "${WORK_DIR}/sub_2.fastq"
        cat "${WORK_DIR}/sub_1.fastq" "${WORK_DIR}/sub_2.fastq" > "${TRIMMED}"
        rm -f "${WORK_DIR}/sub_1.fastq" "${WORK_DIR}/sub_2.fastq"
    else
        cat "${WORK_DIR}/${ACCESSION}_trimmed_1.fastq" \
            "${WORK_DIR}/${ACCESSION}_trimmed_2.fastq" \
            > "${TRIMMED}"
    fi
    rm -f "${WORK_DIR}/${ACCESSION}_trimmed_1.fastq" "${WORK_DIR}/${ACCESSION}_trimmed_2.fastq"
else
    fastp \
        --in1 "${WORK_DIR}/${ACCESSION}.fastq" \
        --out1 "${TRIMMED}" \
        --qualified_quality_phred 20 \
        --length_required 50 \
        --thread "${THREADS}" \
        --json "${WORK_DIR}/${ACCESSION}_fastp.json" \
        --html /dev/null \
        2>&1 | tail -5

    rm -f "${WORK_DIR}/${ACCESSION}.fastq"

    if [[ ${SUBSAMPLE_READS} -gt 0 ]]; then
        SE_LINES=$(( SUBSAMPLE_READS * 2 * 4 ))
        echo "  Subsampling to $(( SUBSAMPLE_READS * 2 )) reads..."
        head -n "${SE_LINES}" "${TRIMMED}" > "${WORK_DIR}/sub_se.fastq"
        mv "${WORK_DIR}/sub_se.fastq" "${TRIMMED}"
    fi
fi

TOTAL_READS=$(( $(wc -l < "${TRIMMED}") / 4 ))
echo "  Reads after trimming: ${TOTAL_READS}"

# =============================================================================
# STEP 3: DIAMOND blastx against NCycDB
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 3: DIAMOND blastx vs NCycDB ==="

NCYC_HITS="${WORK_DIR}/${ACCESSION}_ncyc_hits.tsv"
NCYC_COUNTS="${RESULTS_DIR}/${ACCESSION}_ncyc_counts.tsv"

if [[ -f "${NCYC_DB}" ]]; then
    diamond blastx \
        --query "${TRIMMED}" \
        --db "${NCYC_DB}" \
        --out "${NCYC_HITS}" \
        --evalue "${EVALUE}" \
        --id "${IDENTITY}" \
        --query-cover "${QUERY_COV}" \
        --max-target-seqs "${MAX_TARGETS}" \
        --outfmt "${OUTFMT}" \
        --threads "${THREADS}" \
        --block-size "${BLOCK_SIZE}" \
        --index-chunks "${INDEX_CHUNKS}" \
        2>&1 | tail -5

    echo "  Counting NCycDB hits per gene family..."
    if [[ -s "${NCYC_HITS}" ]]; then
        echo -e "gene_family\thit_count" > "${NCYC_COUNTS}"

        COMBINED_MAP="${WORK_DIR}/combined_id2gene.tsv"
        cp "${NCYC_MAP}" "${COMBINED_MAP}"
        if [[ -f "${NCYC_FAA}" ]]; then
            grep "^>" "${NCYC_FAA}" | \
                sed 's/>//' | \
                awk '{
                    id = $1
                    gene = ""
                    for (i=2; i<=NF; i++) {
                        if ($i ~ /^\[description=/) {
                            gsub(/\[description=/, "", $i)
                            gsub(/\]/, "", $i)
                            gene = $i
                            break
                        }
                    }
                    if (gene != "") print id"\t"gene
                }' >> "${COMBINED_MAP}"
        fi

        awk -F'\t' 'BEGIN {
            while ((getline line < "'"${COMBINED_MAP}"'") > 0) {
                split(line, f, "\t")
                map[f[1]] = f[2]
            }
        }
        {
            gene = map[$2]
            if (gene == "") gene = "unknown"
            print gene
        }' "${NCYC_HITS}" | sort | uniq -c | sort -rn | \
        awk '{print $2"\t"$1}' >> "${NCYC_COUNTS}"
        NCYC_TOTAL=$(awk 'NR>1 {s+=$2} END {print s+0}' "${NCYC_COUNTS}")
        echo "  NCycDB total hits: ${NCYC_TOTAL}"
    else
        echo -e "gene_family\thit_count" > "${NCYC_COUNTS}"
        echo "  NCycDB: no hits found."
        NCYC_TOTAL=0
    fi
else
    echo "  WARNING: NCycDB not found — skipping."
    echo -e "gene_family\thit_count" > "${NCYC_COUNTS}"
    NCYC_TOTAL=0
fi

# =============================================================================
# STEP 4: DIAMOND blastx against PlasticDB
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 4: DIAMOND blastx vs PlasticDB ==="

PLASTIC_HITS="${WORK_DIR}/${ACCESSION}_plastic_hits.tsv"
PLASTIC_COUNTS="${RESULTS_DIR}/${ACCESSION}_plastic_counts.tsv"

if [[ -f "${PLASTIC_DB}" ]]; then
    diamond blastx \
        --query "${TRIMMED}" \
        --db "${PLASTIC_DB}" \
        --out "${PLASTIC_HITS}" \
        --evalue "${EVALUE}" \
        --id "${IDENTITY}" \
        --query-cover "${QUERY_COV}" \
        --max-target-seqs "${MAX_TARGETS}" \
        --outfmt "${OUTFMT}" \
        --threads "${THREADS}" \
        --block-size "${BLOCK_SIZE}" \
        --index-chunks "${INDEX_CHUNKS}" \
        2>&1 | tail -5

    echo "  Counting plastic enzyme hits..."
    PLASTIC_BY_TARGET="${RESULTS_DIR}/${ACCESSION}_plastic_by_target.tsv"
    if [[ -s "${PLASTIC_HITS}" ]]; then
        echo -e "enzyme_id\thit_count" > "${PLASTIC_COUNTS}"
        awk -F'\t' '{print $2}' "${PLASTIC_HITS}" | sort | uniq -c | sort -rn | \
        awk '{print $2"\t"$1}' >> "${PLASTIC_COUNTS}"
        PLASTIC_TOTAL=$(awk 'NR>1 {s+=$2} END {print s+0}' "${PLASTIC_COUNTS}")
        echo "  Plastic enzyme total hits: ${PLASTIC_TOTAL}"

        if [[ -f "${PLASTIC_ANNOT}" ]]; then
            echo -e "target_plastic\thit_count" > "${PLASTIC_BY_TARGET}"
            awk -F'\t' 'BEGIN {
                while ((getline line < "'"${PLASTIC_ANNOT}"'") > 0) {
                    split(line, f, "\t")
                    if (f[1] != "seq_id") map[f[1]] = f[4]
                }
            }
            {
                target = map[$2]
                if (target == "") target = "unmapped"
                print target
            }' "${PLASTIC_HITS}" | sort | uniq -c | sort -rn | \
            awk '{print $2"\t"$1}' >> "${PLASTIC_BY_TARGET}"

            NYLON_HITS=$(awk -F'\t' '$1 == "Nylon" {print $2}' "${PLASTIC_BY_TARGET}")
            echo "  Nylon hits: ${NYLON_HITS:-0}"
        fi
    else
        echo -e "enzyme_id\thit_count" > "${PLASTIC_COUNTS}"
        echo -e "target_plastic\thit_count" > "${PLASTIC_BY_TARGET}"
        echo "  Plastic enzyme DB: no hits found."
        PLASTIC_TOTAL=0
    fi
else
    echo "  WARNING: PlasticDB not found — skipping."
    echo -e "enzyme_id\thit_count" > "${PLASTIC_COUNTS}"
    PLASTIC_TOTAL=0
fi

# =============================================================================
# STEP 5: DIAMOND blastx against extended N-metabolism DB
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 5: DIAMOND blastx vs extended N-metabolism DB ==="

EXTN_HITS="${WORK_DIR}/${ACCESSION}_extN_hits.tsv"
EXTN_COUNTS="${RESULTS_DIR}/${ACCESSION}_extN_counts.tsv"

if [[ -f "${EXTN_DB}" ]]; then
    diamond blastx \
        --query "${TRIMMED}" \
        --db "${EXTN_DB}" \
        --out "${EXTN_HITS}" \
        --evalue "${EVALUE}" \
        --id "${IDENTITY}" \
        --query-cover "${QUERY_COV}" \
        --max-target-seqs "${MAX_TARGETS}" \
        --outfmt "${OUTFMT}" \
        --threads "${THREADS}" \
        --block-size "${BLOCK_SIZE}" \
        --index-chunks "${INDEX_CHUNKS}" \
        2>&1 | tail -5

    echo "  Counting extended N-metabolism hits per gene family..."
    if [[ -s "${EXTN_HITS}" ]]; then
        echo -e "gene_family\thit_count" > "${EXTN_COUNTS}"
        awk -F'\t' 'BEGIN {
            while ((getline line < "'"${EXTN_MAP}"'") > 0) {
                split(line, f, "\t")
                map[f[1]] = f[2]
            }
        }
        {
            sid = $2
            split(sid, parts, "|")
            acc = (parts[2] != "") ? parts[2] : sid
            gene = map[acc]
            if (gene == "") gene = "unknown"
            print gene
        }' "${EXTN_HITS}" | sort | uniq -c | sort -rn | \
        awk '{print $2"\t"$1}' >> "${EXTN_COUNTS}"
        EXTN_TOTAL=$(awk 'NR>1 {s+=$2} END {print s+0}' "${EXTN_COUNTS}")
        echo "  Extended N-metabolism total hits: ${EXTN_TOTAL}"
    else
        echo -e "gene_family\thit_count" > "${EXTN_COUNTS}"
        echo "  Extended N-metabolism DB: no hits found."
        EXTN_TOTAL=0
    fi
else
    echo "  WARNING: Extended N-metabolism DB not found — skipping."
    echo -e "gene_family\thit_count" > "${EXTN_COUNTS}"
    EXTN_TOTAL=0
fi

# =============================================================================
# STEP 6: DIAMOND blastx against functional gene DB
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 6: DIAMOND blastx vs functional gene DB ==="

FUNC_HITS="${WORK_DIR}/${ACCESSION}_func_hits.tsv"
FUNC_COUNTS="${RESULTS_DIR}/${ACCESSION}_func_counts.tsv"

if [[ -f "${FUNC_DB}" ]]; then
    diamond blastx \
        --query "${TRIMMED}" \
        --db "${FUNC_DB}" \
        --out "${FUNC_HITS}" \
        --evalue "${EVALUE}" \
        --id "${IDENTITY}" \
        --query-cover "${QUERY_COV}" \
        --max-target-seqs "${MAX_TARGETS}" \
        --outfmt "${OUTFMT}" \
        --threads "${THREADS}" \
        --block-size "${BLOCK_SIZE}" \
        --index-chunks "${INDEX_CHUNKS}" \
        2>&1 | tail -5

    echo "  Counting functional gene hits per gene family..."
    if [[ -s "${FUNC_HITS}" ]]; then
        echo -e "gene_family\thit_count" > "${FUNC_COUNTS}"
        awk -F'\t' 'BEGIN {
            while ((getline line < "'"${FUNC_MAP}"'") > 0) {
                split(line, f, "\t")
                map[f[1]] = f[2]
            }
        }
        {
            sid = $2
            split(sid, parts, "|")
            acc = (parts[2] != "") ? parts[2] : sid
            gene = map[acc]
            if (gene == "") {
                gene = map[sid]
            }
            if (gene == "") gene = "unknown"
            print gene
        }' "${FUNC_HITS}" | sort | uniq -c | sort -rn | \
        awk '{print $2"\t"$1}' >> "${FUNC_COUNTS}"
        FUNC_TOTAL=$(awk 'NR>1 {s+=$2} END {print s+0}' "${FUNC_COUNTS}")
        echo "  Functional gene total hits: ${FUNC_TOTAL}"
    else
        echo -e "gene_family\thit_count" > "${FUNC_COUNTS}"
        echo "  Functional gene DB: no hits found."
        FUNC_TOTAL=0
    fi
else
    echo "  WARNING: Functional gene DB not found — skipping."
    echo -e "gene_family\thit_count" > "${FUNC_COUNTS}"
    FUNC_TOTAL=0
fi

# =============================================================================
# STEP 7: Save per-sample statistics
# =============================================================================
echo ""
echo "=== [${ACCESSION}] Step 7: Saving statistics ==="

STATS_FILE="${RESULTS_DIR}/${ACCESSION}_stats.tsv"
{
    echo -e "accession\tlayout\ttotal_reads_trimmed\tncyc_hits\tplastic_hits\textN_hits\tfunc_hits"
    echo -e "${ACCESSION}\t${LAYOUT}\t${TOTAL_READS}\t${NCYC_TOTAL}\t${PLASTIC_TOTAL:-0}\t${EXTN_TOTAL:-0}\t${FUNC_TOTAL:-0}"
} > "${STATS_FILE}"

echo "  Stats saved to: ${STATS_FILE}"
echo ""
echo "=== [${ACCESSION}] Pipeline complete ==="
echo "  Total reads (trimmed): ${TOTAL_READS}"
echo "  NCycDB hits:           ${NCYC_TOTAL}"
echo "  PlasticDB hits:        ${PLASTIC_TOTAL:-0}"
echo "  Extended N hits:       ${EXTN_TOTAL:-0}"
echo "  Functional hits:       ${FUNC_TOTAL:-0}"
