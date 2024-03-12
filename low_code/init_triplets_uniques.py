# première étape dans la création de referentiel_definition_incident par la création de triplet_unique
import os
from glob import glob
import pandas as pd
from src.constants import input_folder, encoding, sep


# import data brutes d'incidents par année
input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))

# data définitions de référence
df_ref = pd.DataFrame(
    None,
    columns=[
        "IO - Type d'incident",
        "IO - Type de défaillance",
        "IO - Type de ressource défaillante",
    ],
)

# boucle sur les fichiers de data d'incidents, récupération des triplets uniques, comparaison/concaténation au référentiel
for input_file in input_files:
    df_icd = pd.read_csv(
        input_file,
        sep=sep,
        encoding=encoding,
        usecols=[
            "IO - Type d'incident",
            "IO - Type de défaillance",
            "IO - Type de ressource défaillante",
        ],
    ).drop_duplicates(ignore_index=True)

    df_ref = pd.concat([df_ref, df_icd]).drop_duplicates(ignore_index=True)


# triplets (type incident, type defaillance, type ressource defaillante) uniques, toute année confondue
df_ref.rename(
    columns={
        "type_incident": "IO - Type d'incident",
        "type_def": "IO - Type de défaillance",
        "type_res_def": "IO - Type de ressource défaillante",
    },
    inplace=True,
)

# export triplets_uniques
df_ref.to_csv(
    os.path.join(input_folder, "triplets_uniques.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
)


# l'étape de regroupement / de classification est réalisée à la main
# le fichier avant la classification est mis dans l'input_folder sous le nom : triplets_uniques.csv (ce fichier est généré par le script présent)
# le fichier avec la classification est mis dans l'input_folder sous le nom : referentiel_definition_incident.csv
# triplets cohérents/corrects sont associés à : VOIE, ADV, CAT, EALE, PN, TELECOM, TVX,
# lesquels ne sont pas cohérentes/corrects ou sont hors périmètres : EXT

# suppression du fichier des triplets uniques
# os.remove(os.path.join(input_folder, "triplets_uniques.csv"))
