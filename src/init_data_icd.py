import os
from glob import glob
import numpy as np
import pandas as pd
import logging

from constants import *
from import_export import *
from clean import clean_PR
from matching import matching


def preprocessing_icd(df, date_min, date_max):
    """
    Nettoyage d'un fichier csv pour une année (1 incident par ligne)
    """

    logging.info("start")

    # date de début et de fin d'incident au format datetime
    df["IO - Date de début"] = pd.to_datetime(df["IO - Date de début"], format="mixed")
    df["IO - Date de fin"] = pd.to_datetime(df["IO - Date de fin"], format="mixed")

    # si date_de_debut > date_de_fin, correction date_de_fin en remplaçant par le lendemain de date_de_debut (sans l'heure)
    mask = df["IO - Date de début"].dt.strftime("%Y-%m-%d") > df["IO - Date de fin"]
    df.loc[mask, "IO - Date de fin"] = (
        df["IO - Date de début"] + pd.to_timedelta(1, unit="d")
    ).dt.strftime("%Y-%m-%d")

    # sélectionner les incidents dans l'intervalle de temps
    df = df.loc[
        (df["IO - Date de début"] >= pd.to_datetime(date_min))
        & (df["IO - Date de fin"] <= pd.to_datetime(date_max)),
        :,
    ]

    # ajouter l'heure à la date de début d'incident
    df.loc[:, "EC - Heure"] = pd.to_timedelta(df["EC - Heure"], unit="h")
    df.loc[:, "IO - Date de début"] = (
        df.loc[:, "IO - Date de début"] + df.loc[:, "EC - Heure"]
    )

    # dataframe avec 1 ligne = 1 incident, incident le plus récent gardé, colonne "commentaires" contient le liste de tous les "IO - Commentaires 1"
    df = df.groupby(by="IO - Numéro d'incident", as_index=False).agg(
        date_de_debut_min=pd.NamedAgg(column="IO - Date de début", aggfunc="min"),
        date_de_fin_max=pd.NamedAgg(column="IO - Date de fin", aggfunc="max"),
        type_incident=pd.NamedAgg(column="IO - Type d'incident", aggfunc=lambda x: x),
        type_def=pd.NamedAgg(column="IO - Type de défaillance", aggfunc=lambda x: x),
        type_res_def=pd.NamedAgg(
            column="IO - Type de ressource défaillante", aggfunc=lambda x: x
        ),
        pr_debut=pd.NamedAgg(column="IO - Libellé PR (PR début)", aggfunc=lambda x: x),
        pr_fin=pd.NamedAgg(column="IO - Libellé PR (PR fin)", aggfunc=lambda x: x),
        nb_perturbations_trains=pd.NamedAgg(
            column="Nbre trains impactés", aggfunc="sum"
        ),
        nb_suppressions_trains=pd.NamedAgg(column="Nbre trains suppr", aggfunc="sum"),
        nb_minutes_perdues=pd.NamedAgg(column="Minutes perdues", aggfunc="sum"),
    )
    df.rename(columns={"IO - Numéro d'incident": "numero"}, inplace=True)

    # si une colonne ne contient que les mêmes valeurs, mettre en str, sinon mettre les valeurs dans une liste de str
    def unlist(x):
        if type(x) is str:
            return x
        elif type(x) is np.ndarray:
            x = list(set(x))
            if len(x) == 1:
                return x[0]
            else:
                return x
        else:
            return None

    df["type_incident"] = df["type_incident"].apply(lambda x: unlist(x))
    df["type_def"] = df["type_def"].apply(lambda x: unlist(x))
    df["type_res_def"] = df["type_res_def"].apply(lambda x: unlist(x))
    df["pr_debut"] = df["pr_debut"].apply(lambda x: unlist(x))
    df["pr_fin"] = df["pr_fin"].apply(lambda x: unlist(x))

    # mise en forme des PR (utf-8, en minuscule, sans tiret, sans parenthèse)
    df["pr_debut"] = clean_PR(df, "pr_debut")
    df["pr_fin"] = clean_PR(df, "pr_fin")

    return df


def join_definition_incident(df):
    """
    Jointure avec referentiel_definition_incident, pour rajouter la colonne 'infra'
    '"""

    logging.info("start")

    # import data
    df_def = import_data(input_folder, "referentiel_definition_incident.csv")

    # triplet qui sert de définition d'un incident, correspondance avec le renommage du triplet, jointure et suppression de l'ancien nommage
    df_id = ["type_incident", "type_def", "type_res_def"]
    df_def_id = [
        "IO - Type d'incident",
        "IO - Type de défaillance",
        "IO - Type de ressource défaillante",
    ]
    df = df.merge(df_def, left_on=df_id, right_on=df_def_id).drop(columns=df_def_id)

    return df


def init_data_icd(**kwargs):
    """Création du dataframe total des incidents (contient toutes les années)."""

    logging.info("start")

    input_folder = get_kwargs(kwargs, "input_folder")
    inter_folder = get_kwargs(kwargs, "inter_folder")
    date_min = get_kwargs(kwargs, "date_min")
    date_max = get_kwargs(kwargs, "date_max")

    # récupérer les noms de fichiers d'incidents dans input_folder et créer le df total vide
    input_files = sorted(glob(os.path.join(input_folder, "incidents_*.csv")))
    df_tot = pd.DataFrame(None, columns=cols_icd_preprocess)

    for input_file in input_files:
        # importer le csv pour une année, avec uniquement les colonnes nécessaires au preprocessing
        df_file = import_data(*os.path.split(input_file))

        # preprocess le df de l'iteration (1 accident par ligne)
        df = preprocessing_icd(df_file, date_min, date_max)

        # concaténer avec le df de toutes les années
        df_tot = pd.concat([df_tot, df])

    # faire la jointure avec referentiel_definition_incident, pour rajouter la colonne "infra"
    df_tot = join_definition_incident(df_tot)

    # exporter df icd total
    export_data(df_tot, inter_folder, "infra_incidents_tot.csv")


def correct_data_icd(**kwargs):
    """
    Suppression d'incidents liés à des PR non localisables.
    """

    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")

    # import data référence
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # import data icd
    df_tot = import_data(inter_folder, "infra_incidents_tot.csv")

    # liste des PR non localisables, et suppression icd si pr_debut ou pr_fin est dans cette liste
    PR_non_localisables = list(
        df_ref.loc[df_ref["pr_ref"] == "Non localisable", "pr_"][1:].values
    )
    df_tot = df_tot.loc[
        ~(
            df_tot["pr_debut"].isin(PR_non_localisables)
            | df_tot["pr_fin"].isin(PR_non_localisables)
        )
    ]

    # export data icd
    export_data(df_tot, inter_folder, "infra_incidents_tot.csv")


if __name__ == "__main__":
    # création du dataframe d'incident
    init_data_icd()
    correct_data_icd()
    matching()
