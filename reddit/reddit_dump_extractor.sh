#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"
DUMPS_DIR="$SCRIPT_DIR/dumps"

COMMENTS_SRC="${COMMENTS_SRC:-$SCRIPT_DIR/comments}"
SUBMISSIONS_SRC="${SUBMISSIONS_SRC:-$SCRIPT_DIR/submissions}"
SUBREDDITS="${SUBREDDITS:-ukraine,worldnews,combatfootage}"

mkdir -p "$OUTPUT_DIR"
mkdir -p "$DUMPS_DIR"

process_file() {
    local SRC_PATH="$1"
    local FILENAME="$(basename "$SRC_PATH")"
    local LOCAL_PATH="$DUMPS_DIR/$FILENAME"
    local OUTPUT_FILE="$OUTPUT_DIR/${FILENAME%.zst}.csv"
    local START_TIME=$(date +%s)

    echo "========================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Processing: $FILENAME"
    echo "========================================"

    if [ -f "$OUTPUT_FILE" ] && [ $(wc -l < "$OUTPUT_FILE") -gt 1000 ]; then
        echo "[$(date '+%H:%M:%S')] Skipping $FILENAME — output already exists ($(wc -l < "$OUTPUT_FILE") rows)"
        return
    fi

    if [ -f "$LOCAL_PATH" ]; then
        echo "[$(date '+%H:%M:%S')] Local copy already exists, skipping copy."
    else
        echo "[$(date '+%H:%M:%S')] Copying $FILENAME to local filesystem..."
        cp "$SRC_PATH" "$LOCAL_PATH"
        echo "[$(date '+%H:%M:%S')] Copy done. Size: $(du -h "$LOCAL_PATH" | cut -f1)"
    fi

    echo "[$(date '+%H:%M:%S')] Starting filter..."

    zstd -dc --long=31 "$LOCAL_PATH" | \
        python3 "$SCRIPT_DIR/filter_reddit_dump.py" \
        --subreddits "$SUBREDDITS" \
        --output "$OUTPUT_FILE"

    echo "[$(date '+%H:%M:%S')] Deleting local copy..."
    rm "$LOCAL_PATH"

    local END_TIME=$(date +%s)
    local ELAPSED=$(( END_TIME - START_TIME ))
    local MINS=$(( ELAPSED / 60 ))
    local SECS=$(( ELAPSED % 60 ))
    local SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    local ROWS=$(wc -l < "$OUTPUT_FILE")

    echo "[$(date '+%H:%M:%S')] Finished: $FILENAME | Time: ${MINS}m ${SECS}s | Size: $SIZE | Rows: $ROWS"
    echo "========================================"
}

BATCH_START=$(date +%s)

echo "========================================"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] STARTING BATCH PROCESSING"
echo "Subreddits: $SUBREDDITS"
echo "========================================"

echo ""
echo "=== COMMENTS (RC files) ==="
for F in "$COMMENTS_SRC"/RC_*.zst; do
    [ -f "$F" ] || continue
    process_file "$F"
done
echo "[$(date '+%H:%M:%S')] All comments processed!"

echo ""
echo "=== SUBMISSIONS (RS files) ==="
for F in "$SUBMISSIONS_SRC"/RS_*.zst; do
    [ -f "$F" ] || continue
    process_file "$F"
done
echo "[$(date '+%H:%M:%S')] All submissions processed!"

BATCH_END=$(date +%s)
BATCH_ELAPSED=$(( BATCH_END - BATCH_START ))
BATCH_HOURS=$(( BATCH_ELAPSED / 3600 ))
BATCH_MINS=$(( (BATCH_ELAPSED % 3600) / 60 ))

echo ""
echo "========================================"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALL DONE!"
echo "Total time: ${BATCH_HOURS}h ${BATCH_MINS}m"
echo "Output files:"
ls -lh "$OUTPUT_DIR"
echo "========================================"
