#!/bin/bash
set -e

DATA_DIR="Data"

# Sicherstellen, dass der Ordner existiert
if [ ! -d "$DATA_DIR" ]; then
    echo "Ordner $DATA_DIR nicht gefunden."
    exit 0
fi

# 1. Alle relevanten Dateien finden
# Wir nutzen -path, um die Klammern innerhalb des Befehls sauber zu halten
ALL_FILES=$(find "$DATA_DIR" -type f \( -name "*.csv" -o -name "*.geojson" -o -name "*.parquet" \))

# 2. Dateien finden, die bereits von DVC getrackt werden
TRACKED=$(dvc list . "$DATA_DIR" --dvc-only -R 2>/dev/null | grep -E "\.(csv|geojson|parquet)$" || true)

TO_ADD=()

# 3. Prüfen, welche Dateien neu sind
for file in $ALL_FILES; do
    # Den Dateinamen extrahieren für den Vergleich
    FILENAME=$(basename "$file")

    if ! echo "$TRACKED" | grep -q "$FILENAME"; then
        TO_ADD+=("$file")
    fi
done

# 4. DVC Add und Git Commit
if [ ${#TO_ADD[@]} -gt 0 ]; then
    echo "Gefundene neue Dateien: ${#TO_ADD[@]}"
    for f in "${TO_ADD[@]}"; do
        echo "Adding: $f"
        dvc add "$f"
        # Wir fügen die .dvc Datei und die .gitignore Änderung hinzu
        git add "$f.dvc" "$(dirname "$f")/.gitignore" 2>/dev/null || true
    done

    git commit -m "Auto-add new data files to DVC"
    dvc push
else
    echo "Alle Datensätze sind bereits in DVC erfasst."
fi