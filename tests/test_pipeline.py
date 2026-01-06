# tests/test_pipeline.py
import pytest
import pandas as pd
from datetime import time
from Python.pipeline import Data1Pipeline, Config  # Import deiner Klassen


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def pipeline(config):
    return Data1Pipeline(config)


def test_encode_binary(pipeline):
    # Testdaten erstellen
    df = pd.DataFrame({"reg_bus": ["JA", "Nein", "JA"]})

    # Funktion ausführen
    result = pipeline.encode_binary(df)

    # Prüfen, ob die Umwandlung korrekt ist
    assert result["reg_bus"].tolist() == [1, 0, 1]


def test_aggregate_vehicle_counts(pipeline):
    # Testdaten für Fahrzeuge
    df = pd.DataFrame({
        "Personenwagen": [10],
        "Personenwagen mit anhänger": [2],
        "Lieferwagen": [5],
        "Lieferwagen mit anhänger": [1],
        "Lieferwagen mit auflieger": [0],
        "Lastwagen": [3],
        "Lastwagen mit anhänger": [1],
        "sattelzug": [1],
        "Motorrad": [2],
        "bus": [1]
    })

    result = pipeline.aggregate_vehicle_counts(df)

    assert result.loc[0, "pkw_total"] == 12
    assert result.loc[0, "lkw_total"] == 5
    assert result.loc[0, "motorisiert_total"] == 26


def test_rename_weekdays(pipeline):
    df = pd.DataFrame({"wochentag": ["Monday", "Sunday"]})
    df["wochentag"] = df["wochentag"].astype("category")

    # Wir müssen sicherstellen, dass die Kategorien existieren
    df["wochentag"] = df["wochentag"].cat.set_categories(
        ["Monday", "Sunday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"])

    result = pipeline.rename_weekdays(df)
    assert "Montag" in result["wochentag"].values
    assert "Sonntag" in result["wochentag"].values