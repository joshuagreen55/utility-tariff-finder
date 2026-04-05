#!/bin/bash

BATCHES="7 8 9 10 11 12 13 14 15"
WORKERS=4

declare -A BATCH_STATES
BATCH_STATES[6]="IA,KS,MO,OK"
BATCH_STATES[7]="AR,LA,MS"
BATCH_STATES[8]="AL,GA"
BATCH_STATES[9]="FL,NC,SC"
BATCH_STATES[10]="TN,KY,WV"
BATCH_STATES[11]="WI,MI"
BATCH_STATES[12]="OH,IN,IL"
BATCH_STATES[13]="PA,NJ,VA,MD,DE,DC"
BATCH_STATES[14]="NY"
BATCH_STATES[15]="MA,CT,ME,VT,NH,RI"

declare -A BATCH_NAMES
BATCH_NAMES[6]="Heartland"
BATCH_NAMES[7]="South Central"
BATCH_NAMES[8]="Deep South"
BATCH_NAMES[9]="Southeast"
BATCH_NAMES[10]="Upper South"
BATCH_NAMES[11]="Great Lakes"
BATCH_NAMES[12]="Ohio Valley"
BATCH_NAMES[13]="Mid-Atlantic"
BATCH_NAMES[14]="New York"
BATCH_NAMES[15]="New England"

echo "=============================================="
echo "  AUTOMATED BATCH RUNNER — Batches 6-15"
echo "  Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "  Workers: $WORKERS"
echo "=============================================="

OVERALL_START=$(date +%s)

for B in $BATCHES; do
    STATES="${BATCH_STATES[$B]}"
    NAME="${BATCH_NAMES[$B]}"
    BATCH_START=$(date +%s)

    echo ""
    echo "======================================================"
    echo "  BATCH $B: $NAME ($STATES)"
    echo "  Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "======================================================"

    # Step 1: Run the pipeline
    echo "[Batch $B] Step 1/3: Running pipeline with $WORKERS workers..."
    python3 -m scripts.us_batch_runner \
        --batch "$B" --comprehensive --workers "$WORKERS" \
        2>&1 || echo "[Batch $B] Pipeline exited with code $?"

    # Step 2: Cleanup duplicate tariffs
    echo ""
    echo "[Batch $B] Step 2/3: Cleaning up duplicate tariffs..."
    python3 -m scripts.cleanup_duplicate_tariffs \
        --country US --states "$STATES" \
        2>&1 || echo "[Batch $B] Cleanup exited with code $?"

    # Step 3: Deactivate zero-tariff utilities in this batch's states
    echo ""
    echo "[Batch $B] Step 3/3: Deactivating zero-tariff utilities..."
    IFS=',' read -ra STATE_ARRAY <<< "$STATES"
    STATE_LIST=""
    for S in "${STATE_ARRAY[@]}"; do
        [ -n "$STATE_LIST" ] && STATE_LIST="$STATE_LIST,"
        STATE_LIST="$STATE_LIST'$S'"
    done

    DEACTIVATED=$(python3 -c "
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine
engine = get_sync_engine()
with Session(engine) as s:
    result = s.execute(text(\"\"\"
        UPDATE utilities SET is_active = false
        WHERE state_province IN ($STATE_LIST)
          AND is_active = true
          AND country = 'US'
          AND id NOT IN (SELECT DISTINCT utility_id FROM tariffs)
    \"\"\"))
    s.commit()
    print(result.rowcount)
")
    echo "  Deactivated $DEACTIVATED utilities with zero tariffs"

    BATCH_END=$(date +%s)
    BATCH_ELAPSED=$(( (BATCH_END - BATCH_START) / 60 ))

    echo ""
    echo "------------------------------------------------------"
    echo "  BATCH $B COMPLETE: $NAME — ${BATCH_ELAPSED} minutes"
    echo "  Finished: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "------------------------------------------------------"
done

OVERALL_END=$(date +%s)
OVERALL_ELAPSED=$(( (OVERALL_END - OVERALL_START) / 60 ))

echo ""
echo "=============================================="
echo "  ALL BATCHES COMPLETE"
echo "  Total time: ${OVERALL_ELAPSED} minutes"
echo "  Finished: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "=============================================="

echo ""
echo "Final batch status:"
python3 -m scripts.us_batch_runner --list-batches
