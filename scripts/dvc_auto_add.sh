#!/bin/bash
set -e

DATA_DIR="Data"

# Definiere die unterstützten Dateiendungen (Regex-Format für find)
EXTENSIONS="\( -name *.csv -o -name *.geojson -o -name *.parquet \)"

# 1. Alle relevanten Dateien im Ordner finden
ALL_FILES=$(find "$DATA_DIR" -type f $EXTENSIONS)

# 2. Dateien finden, die bereits von DVC getrackt werden (Endungen extrahieren)
TRACKED=$(dvc list . "$DATA_DIR" --dvc-only -R 2>/dev/null | grep -E "\.(csv|geojson|parquet)$" || true)

TO_ADD=()

# 3. Prüfen, welche Dateien neu sind
for file in $ALL_FILES; do
    # Wir vergleichen den relativen Pfad, um Kollisionen bei gleichen Dateinamen in Unterordnern zu vermeiden
    RELATIVE_PATH=${file#./} # Entfernt ./ am Anfang, falls vorhanden

    if ! echo "$TRACKED" | grep -q "$(basename "$file")"; then
        TO_ADD+=("$file")
    fi
done

# 4. DVC Add und Git Commit
if [ ${#TO_ADD[@]} -gt 0 ]; then
    echo "Gefundene neue Dateien: ${#TO_ADD[@]}"
    for f in "${TO_ADD[@]}"; do
        echo "Adding: $f"
        dvc add "$f"
        git add "$f.dvc"
    done

    git commit -m "Auto-add new data files (csv/geojson/parquet) to DVC"
    dvc push
else
    echo "Alle Datensätze sind bereits in DVC erfasst."
fi