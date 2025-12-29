# SKO_Uebung_1
## Beschreibung
Hier mache ich meinen ersten Imersiv Task von SKO mit dem Code aus VTA

## Ausführung
conda install --file requirements.txt
python src/main.py

## Branching Workflow

Für dieses Projekt verwenden wir einen Feature-Branch-Workflow.

### Hauptbranches
- `main`: enthält ausschliesslich stabilen, lauffähigen Code.
  Direkte Commits auf `main` sind nicht erlaubt.

### Feature-Branches
- Neue Funktionalitäten oder Bugfixes werden in separaten Branches entwickelt.
- Namenskonvention:
  - feature/<beschreibung>
  - bugfix/<beschreibung>

### Entwicklungsprozess
1. Für jede Aufgabe wird ein neuer Branch vom `main`-Branch erstellt.
2. Änderungen werden regelmässig mit aussagekräftigen Commit-Messages committed.
3. Nach Abschluss wird ein Merge Request auf `main` erstellt.
4. Mindestens ein weiteres Teammitglied prüft den Code.
5. Nach erfolgreichem Review wird der Branch gemerged und gelöscht.