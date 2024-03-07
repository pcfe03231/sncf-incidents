# %% création des data de réferentiel sur les années et les caractéristiques de réseau
import pandas as pd
import os

### années
# %%
df_annee = pd.DataFrame(range(2010, 2024), columns=["annee"])

# %%
df_annee.to_csv(
    os.path.join("../data_output2", "referentiel_annee.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

### infrapole
# %% 
df_infrapole = pd.DataFrame(
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

# %%
df_infrapole.to_csv(
    os.path.join("../data_output2", "referentiel_infrapole.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

### uic
# %% 
df_uic = pd.DataFrame(
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

# %%
df_uic.to_csv(
    os.path.join("../data_output2", "referentiel_uic.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

### densité
# %%
df_densite = pd.DataFrame(
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

# %%
df_densite.to_csv(
    os.path.join("../data_output2", "referentiel_densite.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)


### ligne
# %%
df_ligne = pd.read_csv(
    os.path.join("../data_output2", "referentiel_pr.csv"),
    sep="\t",
    encoding="utf-16",
    low_memory=False,
    usecols=["ligne_transilien"]
).drop_duplicates(ignore_index=True).dropna()

# %%
df_ligne.to_csv(
    os.path.join("../data_output2", "referentiel_ligne.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)


### code ligne
# %%
df_codeligne = pd.read_csv(
    os.path.join("../data_output2", "referentiel_pr.csv"),
    sep="\t",
    encoding="utf-16",
    low_memory=False,
    usecols=["code_ligne"]
).drop_duplicates(ignore_index=True)

# %%
icv_files = [
    "sig_icv.csv",
    "voie_icv.csv",
    "eale_icv.csv",
    "adv_icv.csv",
    "cat_icv.csv",
]

for icv_file in icv_files:
    df = pd.read_csv(
        os.path.join("../data_input_icv", icv_file),
        sep="\t",
        encoding="utf-16",
        low_memory=False,
        usecols=["LIGNE"],
    ).drop_duplicates(ignore_index=True).rename(columns={"LIGNE": "code_ligne"})

    df_codeligne = pd.concat([df_codeligne, df])
df_codeligne.drop_duplicates(inplace=True, ignore_index=True)

# %%
df_codeligne.to_csv(
    os.path.join("../data_output2", "referentiel_codeligne.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)


# %%
