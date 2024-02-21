import os
import pytest
from glob import glob
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
