#!/bin/bash
set -e

# --- KONFIGURATION ---
DATA_DIR="Data"
# Unterstützte Dateiendungen
EXTENSIONS_FIND="\( -name *.csv -o -name *.geojson -o -name *.parquet \)"
EXTENSIONS_GREP="\.(csv|geojson|parquet)$"

echo "Starte DVC Auto-Check für Ordner: $DATA_DIR"

# 1. DVC INITIALISIEREN (falls nicht vorhanden)
if [ ! -d ".dvc" ]; then
    echo "DVC ist noch nicht initialisiert. Initialisiere jetzt..."
    dvc init --no-scm
    git add .dvc
    # Hinweis: Remote wird über den GitHub Workflow konfiguriert
fi

# 2. ORDNER-STRUKTUR PRÜFEN
if [ ! -d "$DATA_DIR" ]; then
    echo "Ordner $DATA_DIR existiert nicht. Erstelle Ordner..."
    mkdir -p "$DATA_DIR"
fi

# 3. DATEIEN FINDEN (Lokal vs. DVC)
echo "Suche nach neuen Datensätzen..."
ALL_FILES=$(find "$DATA_DIR" -type f $EXTENSIONS_FIND 2>/dev/null || true)

# Dateien finden, die bereits von DVC getrackt werden
TRACKED=$(dvc list . "$DATA_DIR" --dvc-only -R 2>/dev/null | grep -E "$EXTENSIONS_GREP" || true)

TO_ADD=()

for file in $ALL_FILES; do
    FILENAME=$(basename "$file")
    if ! echo "$TRACKED" | grep -q "$FILENAME"; then
        TO_ADD+=("$file")
    fi
done

# 4. VERARBEITUNG DER NEUEN DATEIEN
if [ ${#TO_ADD[@]} -gt 0 ]; then
    echo "Gefundene neue Dateien: ${#TO_ADD[@]}"

    for f in "${TO_ADD[@]}"; do
        echo "Verarbeite: $f"

        # Konfliktlösung: Falls Datei in Git ist, aus Git-Index entfernen
        if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
            echo "Entferne $f aus Git-Tracking (Migration zu DVC)..."
            git rm --cached "$f"
        fi

        # DVC hinzufügen
        dvc add "$f"

        # .dvc Datei zu Git hinzufügen
        git add "$f.dvc"
    done

    # .gitignore im Data-Ordner sicherstellen (Alles ignorieren außer .dvc)
    echo "*" > "$DATA_DIR/.gitignore"
    echo "!*.dvc" >> "$DATA_DIR/.gitignore"
    echo "!.gitignore" >> "$DATA_DIR/.gitignore"
    git add "$DATA_DIR/.gitignore"

    # Änderungen committen (Nur lokal im Runner)
    git commit -m "Auto-DVC: Neue Daten hinzugefügt und Git-Migration durchgeführt"

    # Hinweis: dvc push und git push erfolgen im GitHub Workflow
    echo "Verarbeitung abgeschlossen."
else
    echo "Alle Datensätze sind bereits in DVC erfasst. Keine Aktion nötig."
fi