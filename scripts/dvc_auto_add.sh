#!/bin/bash
set -e

# --- KONFIGURATION ---
DATA_DIR="Data"
EXTENSIONS_FIND="\( -name *.csv -o -name *.geojson -o -name *.parquet \)"
EXTENSIONS_GREP="\.(csv|geojson|parquet)$"

echo "Starte DVC Auto-Check für Ordner: $DATA_DIR"

# 1. DVC INITIALISIEREN
if [ ! -d ".dvc" ]; then
    echo "DVC ist noch nicht initialisiert. Initialisiere jetzt..."
    dvc init --no-scm
    git add .dvc
fi

# 2. ORDNER-STRUKTUR PRÜFEN
mkdir -p "$DATA_DIR"

# 3. DATEIEN FINDEN
ALL_FILES=$(find "$DATA_DIR" -type f $EXTENSIONS_FIND 2>/dev/null || true)
TRACKED=$(dvc list . "$DATA_DIR" --dvc-only -R 2>/dev/null | grep -E "$EXTENSIONS_GREP" || true)

TO_ADD=()
for file in $ALL_FILES; do
    FILENAME=$(basename "$file")
    if ! echo "$TRACKED" | grep -q "$FILENAME"; then
        TO_ADD+=("$file")
    fi
done

# 4. VERARBEITUNG
if [ ${#TO_ADD[@]} -gt 0 ]; then
    echo "Gefundene neue Dateien: ${#TO_ADD[@]}"
    for f in "${TO_ADD[@]}"; do
        echo "Verarbeite: $f"
        if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
            git rm --cached "$f"
        fi
        dvc add "$f"
        git add "$f.dvc"
    done

    # .gitignore im Data-Ordner sicherstellen
    echo "*" > "$DATA_DIR/.gitignore"
    echo "!*.dvc" >> "$DATA_DIR/.gitignore"
    echo "!.gitignore" >> "$DATA_DIR/.gitignore"
    git add "$DATA_DIR/.gitignore"

    git commit -m "Auto-DVC: Daten zu DVC migriert (OneDrive)"
else
    echo "Keine neuen Dateien für DVC gefunden."
fi