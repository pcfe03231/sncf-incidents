# création des data de réferentiel sur les caractéristiques de réseau (pour viz2)
import pandas as pd
import os
from src.constants import output_folder2, input_icv_folder, encoding, sep


### infrapole

df_infrapole = pd.DataFrame(  # modifier s'il le faut
    data={
        "infrapole": [
            "PE",
            "PN",
            "PSE",
            "PSL",
            "PSO",
        ]
    }
)

df_infrapole.to_csv(
    os.path.join(output_folder2, "referentiel_infrapole.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


### uic

df_uic = pd.DataFrame(  # modifier s'il le faut
    data={
        "uic": [
            "2 à 4",
            "5 à 6",
            "7 à 9 AV",
            "7 à 9 SV",
            "Inconnu",
        ],
    }
)

df_uic.to_csv(
    os.path.join(output_folder2, "referentiel_uic.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


### densité

df_densite = pd.DataFrame(  # modifier s'il le faut
    data={
        "densite": [
            "Dense",
            "Hors Densité",
            "Hyperdense",
            "Standard",
            "Standard +",
        ]
    }
)

df_densite.to_csv(
    os.path.join(output_folder2, "referentiel_densite.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


### ligne

df_ligne = (  # modifier s'il le faut
    pd.read_csv(
        os.path.join(output_folder2, "referentiel_pr.csv"),
        sep=sep,
        encoding=encoding,
        low_memory=False,
        usecols=["ligne_transilien"],
    )
    .drop_duplicates(ignore_index=True)
    .dropna()
)

df_ligne.to_csv(
    os.path.join(output_folder2, "referentiel_ligne.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


### code ligne (intersection des codes lignes dans referentiel_pr et dans les data d'icv)

df_codeligne = pd.read_csv(
    os.path.join(output_folder2, "referentiel_pr.csv"),
    sep=sep,
    encoding=encoding,
    low_memory=False,
    usecols=["code_ligne"],
).drop_duplicates(ignore_index=True)

icv_files = [
    "sig_icv.csv",
    "voie_icv.csv",
    "eale_icv.csv",
    "adv_icv.csv",
    "cat_icv.csv",
]

for icv_file in icv_files:
    df = (
        pd.read_csv(
            os.path.join(input_icv_folder, icv_file),
            sep=sep,
            encoding=encoding,
            low_memory=False,
            usecols=["LIGNE"],
        )
        .drop_duplicates(ignore_index=True)
        .rename(columns={"LIGNE": "code_ligne"})
    )

    df_codeligne = pd.concat([df_codeligne, df])
df_codeligne.drop_duplicates(inplace=True, ignore_index=True)

df_codeligne.to_csv(
    os.path.join(output_folder2, "referentiel_codeligne.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)
