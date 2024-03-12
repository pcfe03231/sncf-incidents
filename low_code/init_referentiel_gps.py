# stocker toutes les coordonnées gps uniques dans referentiel_gps (viz1 et viz2)
import pandas as pd
import os
from src.constants import (
    output_folder1,
    output_folder2,
    input_icv_folder,
    encoding,
    sep,
)

# viz1

df_gps = pd.DataFrame(columns=["X", "Y"])

df_ref = pd.read_csv(
    os.path.join(output_folder1, "referentiel_pr.csv"),
    sep=sep,
    encoding=encoding,
    low_memory=False,
    usecols=["longitude", "latitude"],
)

df_gps[["X", "Y"]] = df_ref[["longitude", "latitude"]].drop_duplicates(
    ignore_index=True
)

df_gps.to_csv(
    os.path.join(output_folder1, "referentiel_gps.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


# viz2

icv_files = [
    "sig_icv.csv",
    "voie_icv.csv",
    "eale_icv.csv",
    "adv_icv.csv",
    "cat_icv.csv",
]

for icv_file in icv_files:
    df = pd.read_csv(
        os.path.join(input_icv_folder, icv_file),
        sep=sep,
        encoding=encoding,
        low_memory=False,
        usecols=["X", "Y"],
    )

    df_gps = pd.concat([df_gps, df])
    df_gps.drop_duplicates(inplace=True, ignore_index=True)


df_gps.to_csv(
    os.path.join(output_folder2, "referentiel_gps.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)
