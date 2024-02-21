import os
import numpy as np
import pandas as pd
import logging

from constants import input_icv_folder
from import_export import *


def join_PR1(df_mtbf, df_icv, key, df_ref, type_infra):
    """Traitements uniquement pour incidents en gare / sur un même PR."""
    
    logging.info("start")

    # data mtbf pour icd sur un même PR
    df_mtbf1 = df_mtbf[df_mtbf["liste_code_ligne"] == "0"]

    # ajouter des code_ligne et un pk associé à chaque pr dans df_mtbf, garder que code_ligne et pk
    # pr_debut = pr_fin
    df_mtbf1 = pd.merge(
        df_mtbf1,
        df_ref[["pr_ref", "code_ligne", "pk"]],  # df_ref_etendu
        left_on="pr_debut_ref",
        right_on="pr_ref",
    ).drop(columns=["pr_ref", "liste_code_ligne"])

    # associer chaque (code_ligne, pk) des data mtbf au data icv, left (outer) join selon la ligne
    # si PKD et PKF utilisés pour localiser une infra
    if type_infra == "L":
        df1 = pd.merge(
            df_icv,
            df_mtbf1[
                [
                    "code_ligne",
                    "pk",
                    "nombre_icd",
                    "ratio_itv_sur_nbicd",
                    "minimum_tbf",
                    "mediane_tbf",
                    "maximum_tbf",
                    "nombre_perturbations_trains",
                    "nombre_suppressions_trains",
                    "nombre_minutes_perdues",
                ]
            ],
            left_on="LIGNE",
            right_on="code_ligne",
        ).sort_values(by=["PKD", "PKF"])

        # gestion des duplications en prenant les PK cohérents entre data icv et mtbf
        # le pk du PR doit être sur une infra, ie entre le PKD et le PKF d'une infra
        df1 = df1[(df1["PKD"] <= df1["pk"]) & (df1["PKF"] >= df1["pk"])]

        # suppression colonnes inutiles
        df1 = df1.drop(columns=["code_ligne", "pk"])

    # associer chaque (code_ligne, pk) des data mtbf au data icv, left (outer) join selon la ligne
    # si PK utilisé pour localiser une infra
    if type_infra == "P":
        df1 = pd.merge(
            df_icv,
            df_mtbf1[
                [
                    "code_ligne",
                    "pk",
                    "nombre_icd",
                    "ratio_itv_sur_nbicd",
                    "minimum_tbf",
                    "mediane_tbf",
                    "maximum_tbf",
                    "nombre_perturbations_trains",
                    "nombre_suppressions_trains",
                    "nombre_minutes_perdues",
                ]
            ],
            left_on="LIGNE",
            right_on="code_ligne",
        ).sort_values(by=["code_ligne", "pk"])
        # remarque : il y a des code_ligne dans les data mtbf pour lesquels il n'y a pas d'infra

        # calcul des distances entre pk d'un icd/d'un pr et pk d'une infra, afin de prendre l'infra avec le pk le plus proche d'un icd/pr
        df1 = df1.assign(dist_pk=np.abs(df1["PK"] - df1["pk"]))

        # couples (code_ligne, pk) uniques, ie les différentes localisations de chaque lieu d'icd
        df_iter = df1[["code_ligne", "pk"]].drop_duplicates(ignore_index=True)

        # boucle sur les lieux d'icd que l'on peut rattachés aux data icv
        for i, icd in df_iter.iterrows():
            code_ligne = icd["code_ligne"]
            pk = icd["pk"]

            # sous-df de df1, qui contient toutes les infras possibles pour un lieu d'icd
            df1_ = df1[(df1["code_ligne"] == code_ligne) & (df1["pk"] == pk)]

            # on prend l'infra la plus proche, en gardant son id
            id_infra = df1_.iloc[df1_["dist_pk"].argmin(), :][key]

            # dans le sous-df, toutes les valeurs sont remplacées par les infos de l'infra la plus proche
            df1[(df1["code_ligne"] == code_ligne) & (df1["pk"] == pk)] = df1[
                (df1["code_ligne"] == code_ligne) & (df1["pk"] == pk)
            ][df1_[key] == id_infra].values[0]

        # suppression colonnes inutiles et duplications
        df1 = df1.drop(columns=["code_ligne", "pk", "dist_pk"]).drop_duplicates(
            ignore_index=True
        )

    return df1


def join_PR2(df_mtbf, df_icv, df_ref, type_infra):
    """Traitements uniquement pour incidents hors gare / sur un couple de PR."""
    
    logging.info("start")

    # data mtbf pour icd sur un couple de PR
    df_mtbf2 = df_mtbf[df_mtbf["liste_code_ligne"] != "0"]

    # ajouter code_ligne et pk_debut (pk_fin) à pr_debut_ref (pr_fin_ref) dans df_mtbf, garder que code_ligne et pk
    # pr_debut
    df_mtbf2 = (
        pd.merge(
            df_mtbf2,
            df_ref[["pr_ref", "code_ligne", "pk"]],
            left_on="pr_debut_ref",
            right_on="pr_ref",
        )
        .drop(columns=["pr_ref"])
        .rename(columns={"code_ligne": "code_ligne_debut", "pk": "pk_debut"})
    )

    # pk_fin
    df_mtbf2 = (
        pd.merge(
            df_mtbf2,
            df_ref[["pr_ref", "code_ligne", "pk"]],
            left_on="pr_fin_ref",
            right_on="pr_ref",
        )
        .drop(columns=["pr_ref"])
        .rename(columns={"code_ligne": "code_ligne_fin", "pk": "pk_fin"})
    )

    # plusieurs codes lignes pour le même couple de PR (car possiblement plusieurs lignes pour 1 même PR)
    # prendre code ligne cohérent entre les 2 PR
    df_mtbf2 = df_mtbf2[df_mtbf2["code_ligne_debut"] == df_mtbf2["code_ligne_fin"]]

    # associer chaque (code_ligne, pk) des data mtbf au data icv, left (outer) join selon ligne
    if type_infra == "L":
        df2 = pd.merge(
            df_icv,
            df_mtbf2[
                [
                    "code_ligne_debut",
                    "pk_debut",
                    "code_ligne_fin",
                    "pk_fin",
                    "nombre_icd",
                    "ratio_itv_sur_nbicd",
                    "minimum_tbf",
                    "mediane_tbf",
                    "maximum_tbf",
                    "nombre_perturbations_trains",
                    "nombre_suppressions_trains",
                    "nombre_minutes_perdues",
                ]
            ],
            left_on="LIGNE",
            right_on="code_ligne_debut",
        ).sort_values(by=["PKD", "PKF"])

        # gestion des duplications en prenant les PK cohérents entre data icv et mtbf
        # les pk d'une infra doivent être entre les pk définis par un couple de PR
        df2 = df2[(df2["PKD"] >= df2["pk_debut"]) & (df2["PKF"] <= df2["pk_fin"])]

    if type_infra == "P":
        df2 = pd.merge(
            df_icv,
            df_mtbf2[
                [
                    "code_ligne_debut",
                    "pk_debut",
                    "code_ligne_fin",
                    "pk_fin",
                    "nombre_icd",
                    "ratio_itv_sur_nbicd",
                    "minimum_tbf",
                    "mediane_tbf",
                    "maximum_tbf",
                    "nombre_perturbations_trains",
                    "nombre_suppressions_trains",
                    "nombre_minutes_perdues",
                ]
            ],
            left_on="LIGNE",
            right_on="code_ligne_debut",
        ).sort_values(by=["PK"])

        # gestion des duplications en prenant les PK cohérents entre data icv et mtbf
        # les pk d'une infra doivent être entre les pk définis par un couple de PR
        df2 = df2[(df2["PK"] >= df2["pk_debut"]) & (df2["PK"] <= df2["pk_fin"])]

    # supprimer colonnes inutiles
    df2 = df2.drop(columns=["code_ligne_debut", "pk_debut", "code_ligne_fin", "pk_fin"])

    return df2


def control_duplicates(df, df_icv, key):
    """Contrôle des potentielles duplications, propres à df1 et propres à df2, séparemment.\n
    Contrôle des potentielles duplications mutuelles entre df1 et df2"""
    
    logging.info("start")

    # dictionnaire pour appliquer la fonction d'agrégation "first" à certaines colonnes
    dict_agg = {
        x: pd.NamedAgg(column=x, aggfunc="first")
        for x in df_icv.columns.tolist()
        if x != key
    }

    # dictionnaire pour appliquer la fonction d'agrégation "sum" à certaines colonnes
    dict_agg.update(
        {
            x: pd.NamedAgg(column=x, aggfunc="sum")
            for x in [
                "nombre_icd",
                "nombre_perturbations_trains",
                "nombre_suppressions_trains",
                "nombre_minutes_perdues",
            ]
        }
    )

    # dictionnaire pour appliquer la fonction d'agrégation "min" à certaines colonnes
    dict_agg.update(
        {
            x: pd.NamedAgg(
                column=x,
                aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmean(x),
            )
            for x in [
                "ratio_itv_sur_nbicd",
                "minimum_tbf",
                "mediane_tbf",
                "maximum_tbf",
            ]
        }
    )

    # groupby avec les fonctions d'agrégations définies dans le dictionnaire
    df_ = df.groupby(by=key, as_index=False, dropna=False).agg(**dict_agg)

    # return df avec les colonnes dans le bon ordre
    return df_[
        df_icv.columns.tolist()
        + [
            "nombre_icd",
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
            "nombre_perturbations_trains",
            "nombre_suppressions_trains",
            "nombre_minutes_perdues",
        ]
    ]


def merge_iter(df, df_icv, key):
    """Transformations intermédiaires sur le sous-dataframe des infras qui réunit data icv + data mtbf (df).\n
    Jointure de ce dataframe aux data icv (df_icv)."""
    
    logging.info("start")

    # mtbf prennent des valeurs manquantes si pas assez d'incident, arrondi pour certaines colonnes quantitatives 
    df.loc[df["nombre_icd"] == 1, ["minimum_tbf", "mediane_tbf", "maximum_tbf"]] = pd.NA
    df.loc[df["nombre_icd"] == 2, ["minimum_tbf", "maximum_tbf"]] = pd.NA
    df["ratio_itv_sur_nbicd"] = 365 / (df["nombre_icd"] + 1)
    df[
        [
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
        ]
    ] = df[
        [
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
        ]
    ].round(2)

    # rajout de df12 aux data icv de l'iteration df_icv_
    df_icv.reset_index(drop=True, inplace=True)
    df_icv = pd.merge(
        df_icv,
        df[
            [
                key,
                "nombre_icd",
                "ratio_itv_sur_nbicd",
                "minimum_tbf",
                "mediane_tbf",
                "maximum_tbf",
                "nombre_perturbations_trains",
                "nombre_suppressions_trains",
                "nombre_minutes_perdues",
            ]
        ],
        left_on=key,
        right_on=key,
        how="left",
    )

    return df_icv


def merge_mtbf_to_icv(output_folder, infra, key, years):
    """Jointure des données mtbf aux données d'icv, pour une infra et une année choisies."""
    
    logging.info("start")
    
    # choix type infra
    if infra in ["VOIE", "ADV", "CAT"]:
        type_infra = "L"
    if infra in ["EALE", "SIG"]:
        type_infra = "P"

    # import data mtbf pour l'infra
    df_mtbf = import_data(output_folder, "mtbf_infra_itv.csv")
    df_mtbf = df_mtbf[df_mtbf["infra"] == infra].drop(columns=["infra"])

    # import data référence de pr
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # import data icv
    df_icv = import_data(input_icv_folder, f"{infra.lower()}_icv.csv")

    # définition du dataframe final contenant les data icv avec les data mtbf appropriées
    df_icv_final = pd.DataFrame(
        columns=df_icv.columns.tolist()
        + [
            "nombre_icd",
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
            "nombre_perturbations_trains",
            "nombre_suppressions_trains",
            "nombre_minutes_perdues",
        ]
    )

    # boucle sur les années
    for y in years:
        # data pour une année
        df_icv_ = df_icv[df_icv["ANNEE"] == y]
        df_mtbf_ = df_mtbf[
            pd.to_datetime(df_mtbf["itv_min"], format="%Y-%m-%d").dt.year == y
        ]

        # data icv et mtbf pour les icd ayant lieu sur 1 PR
        df1 = join_PR1(
            df_mtbf=df_mtbf_,
            df_icv=df_icv_,
            key=key,
            df_ref=df_ref,
            type_infra=type_infra,
        )
        # data icv et mtbf pour les icd ayant lieu entre 2 PR
        df2 = join_PR2(
            df_mtbf=df_mtbf_,
            df_icv=df_icv_,
            df_ref=df_ref,
            type_infra=type_infra,
        )

        # contrôle (intra) s'ils existent des icd répartis sur plusieurs couples de PR et qui se répercutent sur une même infra
        if df1[df1[key].duplicated(keep=False)].shape[0] != 0:
            df1 = control_duplicates(df1, df_icv=df_icv_, key=key)
        if df2[df2[key].duplicated(keep=False)].shape[0] != 0:
            df2 = control_duplicates(df=df2, df_icv=df_icv_, key=key)

        # contrôle (inter) s'ils existent des infras qui ont eu des icd au niveau d'1 PR (df1) et entre 2 PR (df2)
        df12 = pd.concat([df1, df2], axis=0)
        if (
            df1[df1[key].isin(df2[key])].shape[0]
            + df2[df2[key].isin(df1[key])].shape[0]
            != 0
        ):
            df12 = control_duplicates(df=df12, df_icv=df_icv_, key=key)

        # transformations intermédiaires de df12 (data icv+mtbf sur les infra qui ont des icd) et jointure les data icv de l'itération (année y)
        df_icv_ = merge_iter(df=df12, df_icv=df_icv_, key=key)

        # rajout des data icv de l'iteration aux data icv finales réunissant toutes les années
        df_icv_final = pd.concat([df_icv_final, df_icv_])

    # export data finales icv+mtbf
    export_data(df_icv_final, output_folder, f"{infra.lower()}_icv_mtbf.csv")


if __name__ == "__main__":
    merge_mtbf_to_icv(
        output_folder="data_output5",
        infra="VOIE",
        key="ID",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )

    # merge_mtbf_to_icv(
    #     output_folder="data_output5",
    #     infra="ADV",
    #     key="CLE_ADV",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder="data_output5",
    #     infra="CAT",
    #     key="ID",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder="data_output5",
    #     infra="EALE",
    #     key="ID_ARMEN",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder="data_output5",
    #     infra="SIG",
    #     key="ID",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )
