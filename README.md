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
- `develop`: ist der Branch der verwendet wir dum die Grundlegenden Programmirungen zu erstellen. 
  Nach abschluss des Projektes geht es weiter auf den Feature-Branches 

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

## Entwicklungsstandards & Code-Analyse

Für die Codequalität gelten folgende Standards, welche bei Pull Requests überprüft werden:

- PEP8-konformer Python-Code
- Einheitliche automatische Formatierung mit `black`
- Statische Code-Analyse mit `flake8`
- Keine ungenutzten Imports oder Variablen

### Code Reviews
- Direkte Commits auf `main` sind nicht erlaubt
- Jede Änderung erfolgt über einen Merge Request
- Vor dem Merge müssen alle Linting-Checks erfolgreich sein

### Entwicklungsumgebung
Die Entwicklungsumgebung wird über Conda verwaltet:

```bash
conda env create -f environment.yml
conda activate project-env