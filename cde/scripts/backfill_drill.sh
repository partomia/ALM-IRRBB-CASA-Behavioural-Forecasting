#!/usr/bin/env bash
# Load the lakehouse month by month so the gold table has one Iceberg snapshot
# per month-end, just like real monthly CBS loads. Afterwards run the CAI
# backfill (cai/jobs/backfill_alco_history.py) to create the matching ALCO runs.
#
#   ./cde/scripts/backfill_drill.sh                       # last 6 month-ends
#   ./cde/scripts/backfill_drill.sh 2026-03-31 2026-06-30 # explicit dates
#
# Run-time --arg values replace the job's own args, so --db-prefix is passed
# again here. Run the chain back to back: the shared vcluster scales down
# after ~15 min idle and a cold scale-up can take 20+ minutes.

set -euo pipefail

JOB_PREFIX="${JOB_PREFIX:-rsingh-casa-alb}"
DB_PREFIX="${DB_PREFIX:-rsingh_casa_alb}"

if [[ $# -gt 0 ]]; then
  dates=("$@")
else
  read -r -a dates <<< "$(python3 - <<'EOF'
from datetime import date, timedelta
d, out = date.today().replace(day=1), []
for _ in range(6):
    d = d - timedelta(days=1)
    out.append(d.isoformat())
    d = d.replace(day=1)
print(" ".join(reversed(out)))
EOF
)"
fi

run() { cde job run --name "$1" --arg=--db-prefix --arg="${DB_PREFIX}" "${@:2}" --wait; }

for as_of in "${dates[@]}"; do
  echo "==================== as of ${as_of} ($(date '+%H:%M:%S'))"
  run "${JOB_PREFIX}-generate-cbs-bronze" --arg=--as-of --arg="${as_of}"
  run "${JOB_PREFIX}-validate-bronze"
  run "${JOB_PREFIX}-build-silver-daily"
  run "${JOB_PREFIX}-build-gold-weekly"
done

echo ""
echo "Gold snapshots, one per month-end. In Hue (Impala):"
echo "  DESCRIBE HISTORY ${DB_PREFIX}_gold.casa_weekly_balance;"
echo "Now create the matching ALCO runs from a CAI session:"
echo "  python cai/jobs/backfill_alco_history.py --as-of ${dates[*]}"
