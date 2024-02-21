# %% import modules + definition data folder
import os
from glob import glob
import pandas as pd

input_folder = "../data_input"

# %% import data brutes d'incidents par année
input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))

# %% data définitions de référence
df_ref = pd.DataFrame(
    None,
    columns=[
        "IO - Type d'incident",
        "IO - Type de défaillance",
        "IO - Type de ressource défaillante",
    ],
)

# %% boucle sur les fichiers de data d'incidents, récupération des triplets uniques, comparaison/concaténation au référentiel
for input_file in input_files:
    df_icd = pd.read_csv(
        input_file,
        sep="\t",
        encoding="utf-16",
        usecols=[
            "IO - Type d'incident",
            "IO - Type de défaillance",
            "IO - Type de ressource défaillante",
        ],

    ).drop_duplicates(ignore_index=True)
    
    df_ref = pd.concat([df_ref, df_icd]).drop_duplicates(ignore_index=True)


# %% triplets (type incident, type defaillance, type ressource defaillante) uniques, toute année confondue

df_ref.rename(
    columns={
        "type_incident": "IO - Type d'incident",
        "type_def": "IO - Type de défaillance",
        "type_res_def": "IO - Type de ressource défaillante",
    },
    inplace=True,
)


# %% export triplets_uniques

df_ref.to_csv(
    os.path.join(input_folder, "triplets_uniques.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
)

# %%
# regroupement / classification a été réalisée à la main
# le fichier avec la classification a été mis dans data_input sous le nom : referentiel_definition_incident.csv
# triplets cohérents/corrects sont associés à : VOIE, ADV, CAT, EALE, PN, TELECOM, TVX,
# lesquels ne sont pas cohérentes/corrects ou sont hors périmètres : EXT

# %% suppression triplets uniques

os.remove(os.path.join(input_folder, "triplets_uniques.csv"))




# %%
