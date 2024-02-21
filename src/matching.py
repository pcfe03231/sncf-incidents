import os
import numpy as np
import pandas as pd
from skrub import fuzzy_join
import logging

from constants import *
from import_export import *


def matching_pr(**kwargs):
    """
    Appariement des PR des données d'incidents avec les données de référence, afin d'avoir uniquement les données d'incidents en idf.\n
    Permet de faire le lien avec les données de références, pour localiser les incidents (PK et GPS) et les lier à d'autres informations sur le réseau (infrapole, uic, ...).
    """

    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")

    # import data référence (PR pour matching : pr_)
    df_ref = import_data(output_folder, "referentiel_pr.csv").drop_duplicates(
        ignore_index=True
    )

    # import data icd (PR pour matching : pr_debut et pr_fin)
    df_tot = import_data(inter_folder, "infra_incidents_tot.csv")

    # dataframe qui a 1 colonne pr_icd, regroupe les PR début ou fin des données icd en 1 seule colonne
    df_pr = pd.DataFrame(
        df_tot[["pr_debut", "pr_fin"]].stack().reset_index(drop=True),
        columns=["pr_icd"],
    ).drop_duplicates(ignore_index=True)

    # matching de pr_icd (data icd) avec pr_ (data référence)
    df_join = fuzzy_join(
        df_pr,
        df_ref[["pr_", "pr_ref", "en_idf"]],
        left_on="pr_icd",
        right_on="pr_",
        return_score=True,
    )

    # monitoring des scores
    with pd.ExcelWriter(os.path.join(inter_folder, "matching_pr.xlsx")) as writer:
        (
            df_join.loc[
                df_join.en_idf == False, ["pr_icd", "pr_", "en_idf", "matching_score"]
            ]
            .drop_duplicates(ignore_index=True)
            .sort_values(by=["matching_score"])
            .to_excel(writer, sheet_name="hors_IDF", index=False)
        )
        (
            df_join.loc[
                df_join.en_idf == True, ["pr_icd", "pr_", "en_idf", "matching_score"]
            ]
            .drop_duplicates(ignore_index=True)
            .sort_values(by=["matching_score"])
            .to_excel(writer, sheet_name="en_IDF", index=False)
        )

    # suppression colonnes pr_ et matching_score
    df_join = df_join.drop(["pr_", "matching_score"], axis=1)

    # concatenation du matching aux données d'icd, remplacement de pr_debut et pr_fin par leur équivalent pr_ref
    # création colonnes booléennes qui indiquent si pr debut ou fin est en idf
    df_tot = (
        df_tot.merge(
            df_join,
            left_on="pr_debut",
            right_on="pr_icd",
        )
        .merge(
            df_join,
            left_on="pr_fin",
            right_on="pr_icd",
        )
        .drop(columns=["pr_debut", "pr_icd_x", "pr_fin", "pr_icd_y"])
        .rename(
            columns={
                "pr_ref_x": "pr_debut_ref",
                "pr_ref_y": "pr_fin_ref",
                "en_idf_x": "pr_debut_en_idf",
                "en_idf_y": "pr_fin_en_idf",
            }
        )
    )

    # suppression icd hors idf, suppression des colonnes booléennes qui indiquent si pr est en idf
    list_col_en_idf = ["pr_debut_en_idf", "pr_fin_en_idf"]
    df_idf = df_tot.loc[df_tot[list_col_en_idf].all(axis="columns"), :].drop(
        columns=list_col_en_idf
    )

    # export data icd idf
    export_data(df_idf, inter_folder, "infra_incidents_idf.csv")


def matching_code_ligne(**kwargs):
    """
    Appariement des code_ligne des données de référence avec les données d'incidents, afin d'avoir uniquement les données d'incidents en idf.\n
    Permet de faire le lien avec les données de références, pour les lier à d'autres informations sur le réseau (infrapole, uic, ...).
    """

    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")
    is_localization_simplified = get_kwargs(kwargs, "is_localization_simplified")

    # import data référence (PR pour matching : pr_ref)
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # import data icd idf
    df_idf = import_data(inter_folder, "infra_incidents_idf.csv")

    # une colonne PR et une colonne qui liste les codes lignes reliés à un PR
    df_ref_ = df_ref.groupby(by="pr_ref", as_index=False).agg(
        code_ligne=pd.NamedAgg(column="code_ligne", aggfunc=lambda x: list(x))
    )

    # rajout d'une liste de code ligne pour pr_debut_ref et une pour pr_fin_ref
    df_idf = (
        df_idf.merge(df_ref_, left_on="pr_debut_ref", right_on="pr_ref")
        .drop(columns="pr_ref")
        .rename(columns={"code_ligne": "code_ligne_debut"})
    )
    df_idf = (
        df_idf.merge(df_ref_, left_on="pr_fin_ref", right_on="pr_ref")
        .drop(columns="pr_ref")
        .rename(columns={"code_ligne": "code_ligne_fin"})
    )

    # intersection de ces 2 listes
    df_idf = df_idf.assign(
        code_ligne=df_idf[["code_ligne_debut", "code_ligne_fin"]].apply(
            lambda x: list(set(x["code_ligne_debut"]) & set(x["code_ligne_fin"])),
            axis=1,
        )
    ).drop(columns=["code_ligne_debut", "code_ligne_fin"])

    # unlist si la liste n'a qu'un element, mettre valeur manquante si la liste est vide
    def unlist(x):
        if type(x) is list:
            if len(x) == 1:
                return x[0]
            if len(x) == 0:
                return np.NaN
            else:
                return x
        else:
            return x

    df_idf["code_ligne"] = df_idf["code_ligne"].apply(unlist)

    # mettre code_ligne = 0 si pr_debut_ref == pr_fin_ref
    df_idf.loc[df_idf["pr_debut_ref"] == df_idf["pr_fin_ref"], "code_ligne"] = 0

    # supprimer les incidents sans code_ligne, supprimer les décimales dans code_ligne
    df_idf = df_idf.loc[~df_idf["code_ligne"].isna(), :]
    df_idf["code_ligne"] = df_idf["code_ligne"].apply(
        lambda x: str(x).replace(".0", "")
    )

    # supprimer sens de circulation, couple (pr_debut, pr_fin) = (pr_fin, pr_debut), si localisation par couple de PR
    if ~is_localization_simplified:
        df_idf[["pr_debut_ref", "pr_fin_ref"]] = np.sort(
            df_idf[["pr_debut_ref", "pr_fin_ref"]], axis=1
        )

    # export data icd idf
    export_data(df_idf, inter_folder, "infra_incidents_idf.csv")


def post_matching(**kwargs):
    """
    Modifications supplémentaires des données de référentiels PR : suppression ligne si pas de code ligne et mise en forme
    """

    logging.info("start")

    output_folder = get_kwargs(kwargs, "output_folder")

    # import data référence (PR pour matching : pr_ref)
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # supprimer les pr_ref sans code_ligne, mettre code_ligne et pk en entier
    df_ref = df_ref.loc[~df_ref["code_ligne"].isna(), :].astype(
        {"code_ligne": "int", "pk": "int"}
    )

    # export data référence
    export_data(df_ref, output_folder, "referentiel_pr.csv")


def matching(**kwargs):
    matching_pr(**kwargs)
    matching_code_ligne(**kwargs)
    post_matching(**kwargs)
