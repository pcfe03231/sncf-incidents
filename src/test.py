import os
# import pytest
from glob import glob
import pandas as pd

from src.constants import input_folder, usecols_icd
from src.import_export import import_data

def test_definitions():
    """Définitions dans les données d'incidents sont appropriées aux définitions des incidents dans le référentiel."""
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    success_list = []

    df_ref = import_data(
        input_folder,
        "referentiel_definition_incident.csv",
        usecols=[
            "IO - Type d'incident",
            "IO - Type de défaillance",
            "IO - Type de ressource défaillante",
        ],
    )
    n = df_ref.shape[0]

    for input_file in input_files:
        df_icd = import_data(
            *os.path.split(input_file),
            usecols=[
                "IO - Type d'incident",
                "IO - Type de défaillance",
                "IO - Type de ressource défaillante",
            ]
        ).drop_duplicates(ignore_index=True)

        df_icd = pd.concat([df_ref, df_icd]).drop_duplicates(ignore_index=True)

        if df_icd.shape[0] == n:
            success_list.append(True)
        else:
            print(input_file)
            success_list.append(False)

    return all(success_list)
    
test_definitions()