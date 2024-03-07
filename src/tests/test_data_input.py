import os
from glob import glob
import numpy as np
import pandas as pd

from constants import input_folder, usecols_icd
from import_export import import_data


def test_files():
    """Existence des fichiers de données d'incidents."""
    input_files = glob(os.path.join(input_folder, "incidents_*.csv"))
    assert len(input_files) > 0


def test_import():
    """Lecture des données d'incidents."""
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    success_list = []
    for input_file in input_files:
        df_file = import_data(*os.path.split(input_file))

        if (
            (type(df_file) == pd.core.frame.DataFrame)
            and (df_file.shape[0] >= 1)
            and (df_file.shape[1] >= len(usecols_icd))
        ):
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)


def test_columns():
    """Colonnes appropriées dans les données d'incidents."""
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    success_list = []
    for input_file in input_files:
        df_file = import_data(*os.path.split(input_file), usecols=usecols_icd)

        if all(
            [
                True if col in df_file.columns.values.tolist() else False
                for col in usecols_icd
            ]
        ):
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)


def test_definitions():
    """Définitions dans les données d'incidents sont appropriées aux définitions des incidents dans le référentiel."""
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    col = [
        "IO - Type d'incident",
        "IO - Type de défaillance",
        "IO - Type de ressource défaillante",
    ]

    df_ref = import_data(
        input_folder,
        "referentiel_definition_incident.csv",
        usecols=col,
    )
    n = df_ref.shape[0]

    success_list = []

    for input_file in input_files:
        df_icd = import_data(*os.path.split(input_file), usecols=col).drop_duplicates(
            ignore_index=True
        )

        df_icd = pd.concat([df_ref, df_icd]).drop_duplicates(ignore_index=True)

        if df_icd.shape[0] == n:
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)


def test_types():
    """Types de données appropriées dans les données d'incidents."""
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    success_list = []
    usecols_icd_types = [
        np.dtype("O"),  # IO - Numéro d'incident
        np.dtype("<M8[ns]"),  # IO - Date de début
        np.dtype("int64"),  # EC - Heure
        np.dtype("<M8[ns]"),  # IO - Date de fin
        np.dtype("O"),  # IO - Type d'incident
        np.dtype("O"),  # IO - Type de défaillance
        np.dtype("O"),  # IO - Type de ressource défaillante
        np.dtype("O"),  # IO - Libellé PR (PR début)
        np.dtype("O"),  # IO - Libellé PR (PR fin)
        np.dtype("int64"),  # Minutes perdues
        np.dtype("int64"),  # Nbre trains impactés
        np.dtype("int64"),  # Nbre trains suppr
    ]
    schema_types = {
        usecols_icd[i]: usecols_icd_types[i] for i in range(len(usecols_icd))
    }
    
    for input_file in input_files:
        df_file = import_data(*os.path.split(input_file), usecols=usecols_icd)
        df_file["IO - Date de début"] = pd.to_datetime(
            df_file["IO - Date de début"], format="mixed"
        )
        df_file["IO - Date de fin"] = pd.to_datetime(
            df_file["IO - Date de fin"], format="mixed"
        )

        if schema_types.items() <= df_file.dtypes.to_dict().items():
            success_list.append(True)
        else:
            success_list.append(False)

    assert all(success_list)
