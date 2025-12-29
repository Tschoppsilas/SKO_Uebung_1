import logging
import sys
import os
from pathlib import Path
import pandas as pd


# ============================================================
# BASIS-PFAD ERMITTLUNG (GLOBAL FIX)
# ============================================================

def get_project_root():
    """
    Ermittelt den Projekt-Root absolut, egal ob aus Notebook oder Skript.
    """
    try:
        # Wenn als Modul geladen (.py Datei)
        root = Path(__file__).resolve().parent.parent
    except NameError:
        # Falls direkt im Notebook-Zelle ausgeführt
        root = Path.cwd()

    # Sicherheitscheck: Wenn wir in einem Unterordner sind, eine Ebene hoch
    if root.name in ["Notebooks", "Python", "notebooks", "python"]:
        root = root.parent
    return root


PROJECT_ROOT = get_project_root()


# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "pipeline.log"

    # Verhindert mehrfache Handler-Registrierung bei Notebook-Restarts
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode="a", encoding="utf-8"),
                logging.StreamHandler(sys.stdout)
            ]
        )


setup_logging()


# ============================================================
# CONFIG
# ============================================================

class Config:
    def __init__(self):
        # Wir erzwingen absolute Pfade basierend auf dem PROJECT_ROOT
        self.data_dir = PROJECT_ROOT / "Data"
        self.data1_path = self.data_dir / "Data_1_neu.csv"
        self.data2_path = self.data_dir / "Data_2.parquet"

        self.gemeinde_filter = "Weinfelden"
        self.year_filter = 2024

        self.join_keys = ["datum", "zeit_von"]
        self.join_type = "inner"
        self.drop_columns = [
            "jahr_data1", "monat_data1", "startzeit", "endzeit",
            "jahr_data2", "monat_data2", "tag"
        ]
        self.strassen_filter = ["K80", "K75", "Gemeindestrasse"]


# ============================================================
# DATA 1 PIPELINE
# ============================================================

class Data1Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("Data1")

    def load(self):
        self.log.info(f"Lade Data1: {self.cfg.data1_path}")
        # Explizite Prüfung mit hilfreicher Meldung
        if not self.cfg.data1_path.exists():
            raise FileNotFoundError(
                f"\n[FEHLER] Datei nicht gefunden!\n"
                f"Gesuchter Pfad: {self.cfg.data1_path.absolute()}\n"
                f"Bitte prüfe, ob der Ordner 'Data' im Projekt-Root existiert und 'dvc pull' ausgeführt wurde."
            )

        df = pd.read_csv(self.cfg.data1_path)
        df = df[df["gemeinde"] == self.cfg.gemeinde_filter].copy()
        df.dropna(inplace=True)
        return df

    def cast_types(self, df):
        self.log.info("Setze Datentypen für Data1")
        return df.astype({
            "code": "Int64",
            "name": "string",
            "gemeinde": "category",
            "adresse": "string",
            "strasse": "string",
            "richtung": "string",
            "jahr": "Int64",
            "wochentag": "category",
        })

    def encode_binary(self, df):
        self.log.info("Kodierung JA/Nein → 1/0")
        df["reg_bus"] = df["reg_bus"].map({"JA": 1, "Nein": 0})
        return df

    def convert_datetime(self, df):
        self.log.info("Konvertiere Datum/Zeit")
        df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
        df["zeit_von"] = pd.to_datetime(df["zeit_von"].astype(str), errors="coerce").dt.time
        df["zeit_bis"] = pd.to_datetime(df["zeit_bis"], format="%H:%M", errors="coerce").dt.time
        return df

    def rename_columns(self, df):
        self.log.info("Benenne Spalten um")
        return df.rename(columns={
            "reg_bus": "Regional Bus",
            "mr": "Motorrad",
            "pw": "Personenwagen",
            "pw+": "Personenwagen mit anhänger",
            "lief": "Lieferwagen",
            "lief+": "Lieferwagen mit anhänger",
            "lief+aufl.": "Lieferwagen mit auflieger",
            "lw": "Lastwagen",
            "lw+": "Lastwagen mit anhänger",
        })

    def rename_weekdays(self, df):
        self.log.info("Übersetze englische Wochentage → deutsch")
        df["wochentag"] = df["wochentag"].cat.rename_categories({
            "Sunday": "Sonntag",
            "Monday": "Montag",
            "Tuesday": "Dienstag",
            "Wednesday": "Mittwoch",
            "Thursday": "Donnerstag",
            "Friday": "Freitag",
            "Saturday": "Samstag"
        })
        return df

    def derive_time_features(self, df):
        self.log.info("Leite Zeitvariablen ab")
        df["jahr"] = df["datum"].dt.year
        df["monat"] = df["datum"].dt.month
        df["woche"] = df["datum"].dt.isocalendar().week
        df["ist_wochenende"] = df["wochentag"].isin(["Samstag", "Sonntag"])
        return df

    def aggregate_vehicle_counts(self, df):
        self.log.info("Aggregiere Fahrzeugkategorien")
        df["pkw_total"] = df["Personenwagen"] + df["Personenwagen mit anhänger"]
        df["lieferwagen_total"] = (
                df["Lieferwagen"] +
                df["Lieferwagen mit anhänger"] +
                df["Lieferwagen mit auflieger"]
        )
        df["lkw_total"] = (
                df["Lastwagen"] +
                df["Lastwagen mit anhänger"] +
                df["sattelzug"]
        )
        df["motorisiert_total"] = (
                df["Motorrad"] +
                df["bus"] +
                df["pkw_total"] +
                df["lieferwagen_total"] +
                df["lkw_total"]
        )
        return df

    def filter_year(self, df):
        self.log.info(f"Filtere Data1 auf Jahr {self.cfg.year_filter}")
        return df[df["jahr"] == self.cfg.year_filter].copy()

    def drop_unnecessary(self, df):
        self.log.info("Entferne nicht benötigte Spalten")
        cols = [
            "gemeinde", "richtung", "spur_code",
            "Personenwagen", "Personenwagen mit anhänger",
            "Lieferwagen", "Lieferwagen mit anhänger", "Lieferwagen mit auflieger",
            "Lastwagen", "Lastwagen mit anhänger", "sattelzug", "stunde"
        ]
        return df.drop(columns=cols, errors="ignore")

    def run(self):
        self.log.info("Starte DATA1 PIPELINE")
        df = self.load()
        df = self.cast_types(df)
        df = self.encode_binary(df)
        df = self.convert_datetime(df)
        df = self.rename_columns(df)
        df = self.rename_weekdays(df)
        df = self.derive_time_features(df)
        df = self.aggregate_vehicle_counts(df)
        df = self.filter_year(df)
        df = self.drop_unnecessary(df)
        self.log.info("Data1 Pipeline abgeschlossen")
        return df


# ============================================================
# DATA 2 PIPELINE
# ============================================================

class Data2Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("Data2")

    def load(self):
        self.log.info(f"Lade Data2: {self.cfg.data2_path}")
        if not self.cfg.data2_path.exists():
            raise FileNotFoundError(f"Datei nicht gefunden: {self.cfg.data2_path}. Hast du 'dvc pull' ausgeführt?")
        return pd.read_parquet(self.cfg.data2_path)

    def cast_types(self, df):
        self.log.info("Setze Datentypen für Data2")
        return df.astype({
            "messstelle": "category",
            "messstationid": "string",
            "indikator": "category"
        })

    def drop_unnecessary(self, df):
        self.log.info("Entferne Spalte messstationid")
        return df.drop(columns=["messstationid"], errors="ignore")

    def create_time_features(self, df):
        self.log.info("Erstelle Zeitvariablen aus startzeit")
        df["startzeit"] = pd.to_datetime(df["startzeit"], errors="coerce")
        df["jahr"] = df["startzeit"].dt.year
        df["monat"] = df["startzeit"].dt.month
        df["tag"] = df["startzeit"].dt.day
        df["zeit_von"] = df["startzeit"].dt.time
        df["datum"] = pd.to_datetime(df["startzeit"].dt.date)
        return df

    def filter_year(self, df):
        self.log.info(f"Filtere Data2 auf Jahr {self.cfg.year_filter}")
        return df[df["jahr"] == self.cfg.year_filter].copy()

    def filter_gemeinde(self, df):
        self.log.info(f"Filtere Data2 auf Gemeinde '{self.cfg.gemeinde_filter}'")
        return df[df["messstelle"].astype(str).str.contains(self.cfg.gemeinde_filter, case=False, na=False)]

    def run(self):
        self.log.info("Starte DATA2 PIPELINE")
        df = self.load()
        df = self.cast_types(df)
        df = self.drop_unnecessary(df)
        df = self.create_time_features(df)
        df = self.filter_year(df)
        df = self.filter_gemeinde(df)
        self.log.info("Data2 Pipeline abgeschlossen")
        return df


# ============================================================
# MERGE PIPELINE
# ============================================================

class MergePipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("Merge")

    def harmonize(self, df1, df2):
        self.log.info("Harmonisiere Datums- und Zeitformate für Merge")
        for df in (df1, df2):
            df["datum"] = pd.to_datetime(df["datum"]).dt.date
            df["zeit_von"] = pd.to_datetime(df["zeit_von"].astype(str), errors="coerce").dt.strftime("%H:%M")
        return df1, df2

    def merge(self, df1, df2):
        self.log.info("Merging Data1 + Data2")
        return df1.merge(
            df2,
            on=self.cfg.join_keys,
            how=self.cfg.join_type,
            suffixes=("_data1", "_data2")
        )

    def drop_unused(self, df):
        self.log.info("Entferne ungenutzte Spalten nach Merge")
        df.drop(columns=self.cfg.drop_columns, inplace=True, errors="ignore")
        return df

    def filter_streets(self, df):
        self.log.info("Erstelle gefilterte DataFrames pro Strasse")
        result = {}
        for street in self.cfg.strassen_filter:
            result[street] = df[df["strasse"] == street].copy()
            self.log.info(f"{street}: {len(result[street])} Zeilen")
        return result

    def run(self, df1, df2):
        self.log.info("Starte MERGE PIPELINE")
        df1, df2 = self.harmonize(df1, df2)
        merged = self.merge(df1, df2)
        merged = self.drop_unused(merged)
        streets = self.filter_streets(merged)
        self.log.info("Merge Pipeline abgeschlossen")
        return merged, streets