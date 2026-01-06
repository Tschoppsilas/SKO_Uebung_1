import pytest
import pandas as pd
import sys
from pathlib import Path

# Sicherstellen, dass der "Python"-Ordner im Systempfad ist
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir / "Python"))

from pipeline import Data1Pipeline, Config


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def pipeline(config):
    return Data1Pipeline(config)


def test_encode_binary(pipeline):
    df = pd.DataFrame({"reg_bus": ["JA", "Nein"]})
    result = pipeline.encode_binary(df)
    assert result["reg_bus"].tolist() == [1, 0]


def test_aggregate_vehicle_counts(pipeline):
    df = pd.DataFrame(
        {
            "Personenwagen": [10],
            "Personenwagen mit anhänger": [2],
            "Lieferwagen": [5],
            "Lieferwagen mit anhänger": [1],
            "Lieferwagen mit auflieger": [0],
            "Lastwagen": [3],
            "Lastwagen mit anhänger": [1],
            "sattelzug": [1],
            "Motorrad": [2],
            "bus": [1],
        }
    )
    result = pipeline.aggregate_vehicle_counts(df)
    assert result.loc[0, "pkw_total"] == 12
    assert result.loc[0, "motorisiert_total"] == 26
