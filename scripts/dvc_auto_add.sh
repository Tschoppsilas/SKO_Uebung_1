#!/bin/bash
set -e

DATA_DIR="Data"

# Alle CSV-Dateien im Ordner
ALL_CSV=$(find $DATA_DIR -name "*.csv")

# Dateien, die schon in DVC getrackt werden
TRACKED=$(dvc list . $DATA_DIR --dvc-only 2>/dev/null | grep ".csv" || true)

TO_ADD=()
for file in $ALL_CSV; do
    if ! echo "$TRACKED" | grep -q "$(basename $file)"; then
        TO_ADD+=("$file")
    fi
done

if [ ${#TO_ADD[@]} -gt 0 ]; then
    echo "Adding new CSVs to DVC..."
    for f in "${TO_ADD[@]}"; do
        dvc add "$f"
        git add "$f.dvc"
    done
    git commit -m "Auto-add new CSVs to DVC"
    dvc push
else
    echo "All CSVs are already tracked in DVC."
fi
