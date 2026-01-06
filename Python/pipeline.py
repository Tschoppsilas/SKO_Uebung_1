import logging
import sys
from pathlib import Path
import pandas as pd


# ============================================================
# BASIS-PFAD ERMITTLUNGEN
# ============================================================


def find_project_root():
    """Sucht den Projekt-Root basierend auf der Existenz des Data-Ordners."""
    try:
        current = Path(__file__).resolve().parent
    except NameError:
        current = Path.cwd()

    for _ in range(4):
        if (current / "Data").exists() or (current / "data").exists():
            return current
        current = current.parent
    return Path.cwd().parent


PROJECT_ROOT = find_project_root()


# ============================================================
# LOGGING SETUP
# ============================================================


def setup_logging():
    """Initialisiert das Logging-System."""
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "pipeline.log"

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


setup_logging()


# ============================================================
# CONFIG
# ============================================================


class Config:
    """Konfiguration mit absoluten Pfaden."""

    def __init__(self):
        d_name = "Data" if (PROJECT_ROOT / "Data").exists() else "data"
        self.data_dir = PROJECT_ROOT / d_name
        self.data1_path = self.data_dir / "Data_1_neu.csv"
        self.data2_path = self.data_dir / "Data_2.parquet"
        self.gemeinde_filter = "Weinfelden"
        self.year_filter = 2024
        self.join_keys = ["datum", "zeit_von"]
        self.join_type = "inner"
        self.drop_columns = [
            "jahr_data1",
            "monat_data1",
            "startzeit",
            "endzeit",
            "jahr_data2",
            "monat_data2",
            "tag",
        ]
        self.strassen_filter = ["K80", "K75", "Gemeindestrasse"]


# ============================================================
# PIPELINES
# ============================================================


class Data1Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("Data1")

    def load(self):
        self.log.info(f"Lade Data1: {self.cfg.data1_path}")
        if not self.cfg.data1_path.exists():
            raise FileNotFoundError(f"Datei fehlt: {self.cfg.data1_path}")
        df = pd.read_csv(self.cfg.data1_path)
        df = df[df["gemeinde"] == self.cfg.gemeinde_filter].copy()
        df.dropna(inplace=True)
        return df

    def cast_types(self, df):
        return df.astype(
            {
                "code": "Int64",
                "name": "string",
                "gemeinde": "category",
                "adresse": "string",
                "strasse": "string",
                "richtung": "string",
                "jahr": "Int64",
                "wochentag": "category",
            }
        )

    def encode_binary(self, df):
        df["reg_bus"] = df["reg_bus"].map({"JA": 1, "Nein": 0})
        return df

    def convert_datetime(self, df):
        df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
        df["zeit_von"] = pd.to_datetime(
            df["zeit_von"].astype(str), errors="coerce"
        ).dt.time
        df["zeit_bis"] = pd.to_datetime(
            df["zeit_bis"], format="%H:%M", errors="coerce"
        ).dt.time
        return df

    def rename_columns(self, df):
        return df.rename(
            columns={
                "reg_bus": "Regional Bus",
                "mr": "Motorrad",
                "pw": "Personenwagen",
                "pw+": "Personenwagen mit anhänger",
                "lief": "Lieferwagen",
                "lief+": "Lieferwagen mit anhänger",
                "lief+aufl.": "Lieferwagen mit auflieger",
                "lw": "Lastwagen",
                "lw+": "Lastwagen mit anhänger",
            }
        )

    def rename_weekdays(self, df):
        df["wochentag"] = df["wochentag"].cat.rename_categories(
            {
                "Sunday": "Sonntag",
                "Monday": "Montag",
                "Tuesday": "Dienstag",
                "Wednesday": "Mittwoch",
                "Thursday": "Donnerstag",
                "Friday": "Freitag",
                "Saturday": "Samstag",
            }
        )
        return df

    def derive_time_features(self, df):
        df["jahr"] = df["datum"].dt.year
        df["monat"] = df["datum"].dt.month
        df["woche"] = df["datum"].dt.isocalendar().week
        df["ist_wochenende"] = df["wochentag"].isin(["Samstag", "Sonntag"])
        return df

    def aggregate_vehicle_counts(self, df):
        df["pkw_total"] = (
            df["Personenwagen"] + df["Personenwagen mit anhänger"]
        )
        df["lieferwagen_total"] = (
            df["Lieferwagen"]
            + df["Lieferwagen mit anhänger"]
            + df["Lieferwagen mit auflieger"]
        )
        df["lkw_total"] = (
            df["Lastwagen"] + df["Lastwagen mit anhänger"] + df["sattelzug"]
        )
        df["motorisiert_total"] = (
            df["Motorrad"]
            + df["bus"]
            + df["pkw_total"]
            + df["lieferwagen_total"]
            + df["lkw_total"]
        )
        return df

    def filter_year(self, df):
        return df[df["jahr"] == self.cfg.year_filter].copy()

    def drop_unnecessary(self, df):
        cols = ["gemeinde", "richtung", "spur_code", "stunde"]
        return df.drop(columns=cols, errors="ignore")

    def run(self):
        df = self.load()
        df = self.cast_types(df)
        df = self.encode_binary(df)
        df = self.convert_datetime(df)
        df = self.rename_columns(df)
        df = self.rename_weekdays(df)
        df = self.derive_time_features(df)
        df = self.aggregate_vehicle_counts(df)
        df = self.filter_year(df)
        return self.drop_unnecessary(df)


class Data2Pipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.log = logging.getLogger("Data2")

    def load(self):
        if not self.cfg.data2_path.exists():
            raise FileNotFoundError(f"Datei fehlt: {self.cfg.data2_path}")
        return pd.read_parquet(self.cfg.data2_path)

    def cast_types(self, df):
        return df.astype(
            {
                "messstelle": "category",
                "messstationid": "string",
                "indikator": "category",
            }
        )

    def create_time_features(self, df):
        df["startzeit"] = pd.to_datetime(df["startzeit"], errors="coerce")
        df["jahr"] = df["startzeit"].dt.year
        df["zeit_von"] = df["startzeit"].dt.time
        df["datum"] = pd.to_datetime(df["startzeit"].dt.date)
        return df

    def run(self):
        df = self.load()
        df = self.cast_types(df)
        df = self.create_time_features(df)
        df = df[df["jahr"] == self.cfg.year_filter].copy()
        mask = (
            df["messstelle"]
            .astype(str)
            .str.contains(self.cfg.gemeinde_filter, case=False, na=False)
        )
        return df[mask].copy()


class MergePipeline:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def run(self, df1, df2):
        for df in (df1, df2):
            df["datum"] = pd.to_datetime(df["datum"]).dt.date
            df["zeit_von"] = pd.to_datetime(
                df["zeit_von"].astype(str)
            ).dt.strftime("%H:%M")

        merged = df1.merge(
            df2,
            on=self.cfg.join_keys,
            how=self.cfg.join_type,
            suffixes=("_d1", "_d2"),
        )
        merged.drop(
            columns=self.cfg.drop_columns, inplace=True, errors="ignore"
        )

        streets = {
            s: merged[merged["strasse"] == s].copy()
            for s in self.cfg.strassen_filter
        }
        return merged, streets


# ============================================================
# DER "BUILD" / EXECUTION TEIL
# ============================================================

if __name__ == "__main__":
    """
    Dieser Block wird nur ausgeführt, wenn man 'python pipeline.py' aufruft.
    Wenn pytest die Datei importiert, wird dieser Teil ignoriert.
    """
    setup_logging()
    logger = logging.getLogger("Main")

    try:
        config = Config()

        # 1. Verkehrsdaten verarbeiten
        p1 = Data1Pipeline(config)
        df_traffic = p1.run()

        # 2. Luftdaten verarbeiten
        p2 = Data2Pipeline(config)
        df_air = p2.run()

        # 3. Mergen
        p_merge = MergePipeline(config)
        final_df, street_dfs = p_merge.run(df_traffic, df_air)

        logger.info("Pipeline erfolgreich durchlaufen!")
        print(f"\nVorschau der Daten:\n{final_df.head()}")

    except Exception as e:
        logger.error(f"Pipeline fehlgeschlagen: {e}", exc_info=True)
        sys.exit(1)
