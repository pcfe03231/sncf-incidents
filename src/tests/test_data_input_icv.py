import os
import pytest
from glob import glob
import pandas as pd

from constants import input_icv_folder
from import_export import import_data

def test_files_icv():
    """Existence fichier de données d'icv."""
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
    """Lecture des données d'icv."""

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
    """Colonnes appropriées dans les données d'icv : pas de colonnes 'criteres' et 'valeur'."""

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
