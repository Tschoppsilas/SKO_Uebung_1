import pytest
import pandas as pd
import sys
from pathlib import Path

# Dieser Teil stellt sicher, dass der Ordner "Python" gefunden wird,
# egal von wo aus der Test gestartet wird.
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir / "Python"))

# Import der Klassen aus deiner pipeline.py
from pipeline import Data1Pipeline, Config


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def pipeline(config):
    return Data1Pipeline(config)


def test_encode_binary(pipeline):
    """Prüft, ob JA/Nein korrekt in 1/0 umgewandelt wird."""
    df = pd.DataFrame({"reg_bus": ["JA", "Nein"]})
    result = pipeline.encode_binary(df)
    assert result["reg_bus"].tolist() == [1, 0]


def test_aggregate_vehicle_counts(pipeline):
    """Prüft, ob die Fahrzeugsummen korrekt berechnet werden."""
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
    # 10 + 2 = 12
    assert result.loc[0, "pkw_total"] == 12
    # 10+2 + 5+1+0 + 3+1+1 + 2+1 = 26
    assert result.loc[0, "motorisiert_total"] == 26


def test_rename_weekdays(pipeline):
    """Prüft, ob Wochentage übersetzt werden."""
    df = pd.DataFrame({"wochentag": ["Monday", "Sunday"]})
    df["wochentag"] = df["wochentag"].astype("category")
    df["wochentag"] = df["wochentag"].cat.set_categories(
        ["Monday", "Sunday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"])

    result = pipeline.rename_weekdays(df)
    assert "Montag" in result["wochentag"].values
    assert "Sonntag" in result["wochentag"].values