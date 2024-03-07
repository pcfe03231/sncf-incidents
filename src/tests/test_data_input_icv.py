import os
import pytest
from glob import glob
import numpy as np
import pandas as pd

from constants import input_icv_folder
from import_export import import_data


def test_files_icv():
    """Existence fichier de données de vieillissement."""
    assert all(
        [
            os.path.isfile(os.path.join(input_icv_folder, "adv_icv.csv")),
            os.path.isfile(os.path.join(input_icv_folder, "cat_icv.csv")),
            os.path.isfile(os.path.join(input_icv_folder, "eale_icv.csv")),
            os.path.isfile(os.path.join(input_icv_folder, "sig_icv.csv")),
            os.path.isfile(os.path.join(input_icv_folder, "voie_icv.csv")),
        ]
    )


def test_import_icv():
    """Lecture des données de vieillissement."""

    success_list = []
    for input_file in glob(os.path.join(input_icv_folder, "*.csv")):

        df_file = import_data(*os.path.split(input_file), low_memory=False)
        if (
            (type(df_file) == pd.core.frame.DataFrame)
            and (df_file.shape[0] >= 1)
            and (df_file.shape[1] >= 5)
        ):
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)


def test_columns_icv():
    """Colonnes appropriées dans les données de vieillissement : pas de colonnes 'criteres' et 'valeur'."""

    success_list = []
    for input_file in glob(os.path.join(input_icv_folder, "*.csv")):
        df_file = import_data(*os.path.split(input_file), low_memory=False)

        if all(
            [
                (
                    True
                    if col not in ["CRITERES", "VALEUR", "Critères", "Valeur"]
                    else False
                )
                for col in df_file.columns.values.tolist()
            ]
        ):
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)


# schema des types de colonnes nécessaires, voie
schema_types_voie = {
    "ID": np.dtype("int64"),
    "X": np.dtype("float64"),
    "Y": np.dtype("float64"),
    "PKD": np.dtype("int64"),
    "PKF": np.dtype("int64"),
    "LONGUEUR": np.dtype("int64"),
    "ICV_VOIE": np.dtype("float64"),
    "ANNEE": np.dtype("int64"),
    "VOIE": np.dtype("O"),
    "IPOLE": np.dtype("O"),
    "GR_UIC": np.dtype("O"),
    "DENSITE_IDF": np.dtype("O"),
    "LIGNE_TRANSILIEN": np.dtype("O"),
    "LIGNE": np.dtype("int64"),
}

# schema des types de colonnes nécessaires, adv
schema_types_adv = {
    "CLE_ADV": np.dtype("O"),
    "X": np.dtype("float64"),
    "Y": np.dtype("float64"),
    "PKD": np.dtype("int64"),
    "PKF": np.dtype("int64"),
    "ICV": np.dtype("float64"),
    "IPOLE": np.dtype("O"),
    "GR_UIC": np.dtype("O"),
    "DENSITE_IDF": np.dtype("O"),
    "LIGNE_TRANSILIEN": np.dtype("O"),
    "LIGNE": np.dtype("int64"),
}

# schema des types de colonnes nécessaires, cat
schema_types_cat = {
    "ID": np.dtype("int64"),
    "X": np.dtype("float64"),
    "Y": np.dtype("float64"),
    "PKD": np.dtype("int64"),
    "PKF": np.dtype("int64"),
    "ICV": np.dtype("float64"),
    "VAL": np.dtype("O"),
    "KM_VOIE": np.dtype("O"),
    "IPOLE": np.dtype("O"),
    "GR_UIC": np.dtype("O"),
    "DENSITE_IDF": np.dtype("O"),
    "LIGNE_TRANSILIEN": np.dtype("O"),
    "LIGNE": np.dtype("int64"),
}

# schema des types de colonnes nécessaires, eale
schema_types_eale = {
    "ID_ARMEN": np.dtype("int64"),
    "X": np.dtype("float64"),
    "Y": np.dtype("float64"),
    "PK": np.dtype("int64"),
    "ICV": np.dtype("float64"),
    "IPOLE": np.dtype("O"),
    "GR_UIC": np.dtype("O"),
    "DENSITE_IDF": np.dtype("O"),
    "LIGNE_TRANSILIEN": np.dtype("O"),
    "LIGNE": np.dtype("int64"),
}

# schema des types de colonnes nécessaires, sig
schema_types_sig = {
    "ID": np.dtype("int64"),
    "X": np.dtype("float64"),
    "Y": np.dtype("float64"),
    "PK": np.dtype("int64"),
    "AGE_MOYEN_AS_GLOBAL": np.dtype("float64"),
    "IPOLE": np.dtype("O"),
    "GR_UIC": np.dtype("O"),
    "DENSITE_IDF": np.dtype("O"),
    "LIGNE_TRANSILIEN": np.dtype("O"),
    "LIGNE": np.dtype("int64"),
}

@pytest.mark.parametrize(
    "infra, schema_types",
    [
        ("voie", schema_types_voie),
        ("adv", schema_types_adv),
        ("cat", schema_types_cat),
        ("eale", schema_types_eale),
        ("sig", schema_types_sig),
    ],
)
def test_types_icv(infra, schema_types):
    """Types de colonnes appropriées dans les données de vieillissement."""
    df = import_data(input_icv_folder, f"{infra}_icv.csv", low_memory=False)

    assert schema_types.items() <= df.dtypes.to_dict().items()
