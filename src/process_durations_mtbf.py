import os
import numpy as np
import pandas as pd
import re
import logging

from constants import *
from import_export import *


def process_durations(df):
    """
    Calcul des durées entre date de début d'incident, pour chaque triplet (pr_debut, pr_fin, code_ligne).
    """
    
    logging.info("start")

    # création colonne qui stocke la durée et qui rajoute une information à l'incident qui a rompu le temps sans incident)
    df = df.assign(durees=np.nan)
    df.sort_values("date_de_debut_min", inplace=True)

    # boucle sur les couples (item) de PR unique, calcul des durées entre 2 incidents pour un même couple
    for i, item in (
        df[
            [
                "pr_debut_ref",
                "pr_fin_ref",
            ]
        ]
        .drop_duplicates(ignore_index=True)
        .iterrows()
    ):
        mask_pr = (df["pr_debut_ref"] == item["pr_debut_ref"]) & (
            df["pr_fin_ref"] == item["pr_fin_ref"]
        )  #
        df.loc[mask_pr, "durees"] = (
            df.loc[mask_pr, "date_de_debut_min"].diff().dt.total_seconds()
            / (60 * 60 * 24)
        ).round(2)

    return df


def process_mtbf(df, **kwargs):
    """
    Calcul des indicateurs à partir des durées entre les incidents, pour chaque couple (pr_debut, pr_fin).\n
    Plusieurs MTBF parmis les indicateurs : durées Minimales/Médianes/Maximales entre 2 incidents.
    """
    
    logging.info("start")

    date_min = get_kwargs(kwargs, "date_min")
    date_max = get_kwargs(kwargs, "date_max")
    is_localization_simplified = get_kwargs(kwargs, "is_localization_simplified")

    df = df.groupby(
        by=["pr_debut_ref", "pr_fin_ref"],
        as_index=False,
    ).agg(
        nombre_icd=pd.NamedAgg(column="numero", aggfunc=lambda x: len(list(x))),
        minimum_tbf=pd.NamedAgg(
            column="durees",
            aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmin(x),
        ),
        mediane_tbf=pd.NamedAgg(
            column="durees",
            aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmedian(x),
        ),
        maximum_tbf=pd.NamedAgg(
            column="durees",
            aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmax(x),
        ),
        nombre_perturbations_trains=pd.NamedAgg(
            column="nb_perturbations_trains", aggfunc="sum"
        ),
        nombre_suppressions_trains=pd.NamedAgg(
            column="nb_suppressions_trains", aggfunc="sum"
        ),
        nombre_minutes_perdues=pd.NamedAgg(column="nb_minutes_perdues", aggfunc="sum"),
        liste_code_ligne=pd.NamedAgg(
            column="code_ligne", aggfunc=lambda x: ", ".join(np.unique(x).tolist())
        ),
    )

    if is_localization_simplified:
        df = df.groupby(
            by=["pr_debut_ref"],
            as_index=False,
        ).agg(
            pr_fin_ref=pd.NamedAgg(
                column="pr_fin_ref", aggfunc=lambda x: ", ".join(np.unique(x))
            ),
            nombre_icd=pd.NamedAgg(column="nombre_icd", aggfunc="sum"),
            minimum_tbf=pd.NamedAgg(
                column="minimum_tbf",
                aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmin(x),
            ),
            mediane_tbf=pd.NamedAgg(
                column="mediane_tbf",
                aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmedian(x),
            ),
            maximum_tbf=pd.NamedAgg(
                column="maximum_tbf",
                aggfunc=lambda x: np.nan if np.all(x != x) else np.nanmax(x),
            ),
            nombre_perturbations_trains=pd.NamedAgg(
                column="nombre_perturbations_trains", aggfunc="sum"
            ),
            nombre_suppressions_trains=pd.NamedAgg(
                column="nombre_suppressions_trains", aggfunc="sum"
            ),
            nombre_minutes_perdues=pd.NamedAgg(
                column="nombre_minutes_perdues", aggfunc="sum"
            ),
            liste_code_ligne=pd.NamedAgg(
                column="liste_code_ligne",
                aggfunc=lambda x: list(
                    set(
                        [
                            b
                            for a in x
                            for b in (eval(a) if type(eval(a)) == list else [eval(a)])
                        ]
                    )
                ),
            ),
        )
    
    # nettoyage de liste_code_ligne
    df["liste_code_ligne"] = df["liste_code_ligne"].apply(
        lambda x: re.sub(r"\[|\]|\{|\}", "", str(x)).strip()
    )

    # NA = non available, valeur manquante
    # si 1 icd alors les 3 mtbf sont NA (pas de durée),
    # si 2 icd alors minimum_tbf et maximum_tbf sont NA (1 seule durée, qui vaut la médiane)
    df.loc[df["nombre_icd"] == 2, ["minimum_tbf", "maximum_tbf"]] = pd.NA
    # si 3 icd alors pas de NA (2 durées, la médiane est une moyenne entre le minimum_tbf et le maximum_tbf)

    # ajout colonne ratio temps total du temps total de l'intervalle défini, sur le nombre d'incident + 1
    df.insert(
        3,
        "ratio_itv_sur_nbicd",
        (date_max - date_min).days / (df["nombre_icd"] + 1),
    )

    # ajouts colonnes qui définissent l'intervalle de temps des traitements
    df = df.assign(itv_min=date_min, itv_max=date_max - datetime.timedelta(days=1))

    # valeurs quantitatives arrondies au centième
    df[
        [
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
            "nombre_perturbations_trains",
            "nombre_suppressions_trains",
            "nombre_minutes_perdues",
        ]
    ] = df[
        [
            "ratio_itv_sur_nbicd",
            "minimum_tbf",
            "mediane_tbf",
            "maximum_tbf",
            "nombre_perturbations_trains",
            "nombre_suppressions_trains",
            "nombre_minutes_perdues",
        ]
    ].round(
        2
    )

    return df


def process_tot(**kwargs):
    """
    Création du dataframe avec les durées, 1 ligne = 1 durée entre icd, avec les carac de l'icd qui a stoppé la durée sans incident).\n
    Création du dataframe avec les MTBF, 1 ligne = 1 triplet (pr_debut, pr_fin, code_ligne).\n
    Les durées (et les MTBF) sont calculées sans distinction de la définition des infrastructures concernés par les incidents.
    """
    
    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")

    # import data icd
    df_icd = import_data(inter_folder, "infra_incidents_idf.csv")

    # nettoyage date de début
    df_icd["date_de_debut_min"] = pd.to_datetime(
        df_icd["date_de_debut_min"], format="mixed"
    )

    # calcul des durées avec les info sur l'incident qui a stoppé la durée sans incident
    df_durees = process_durations(df_icd)

    # calcul des mtbf
    df_mtbf = process_mtbf(df_durees, **kwargs)
    df_mtbf = df_mtbf[cols_mtbf_tot]

    # export data durées et mtbf sans distinction de l'infra
    export_data(df_durees, inter_folder, "durations_tot.csv")
    export_data(df_mtbf, inter_folder, "mtbf_tot.csv")


def process_infra(**kwargs):
    """
    Création du dataframe avec les durées, 1 ligne = 1 durée entre icd, avec les carac de l'icd qui a stoppé la durée sans incident).\n
    Création du dataframe avec les MTBF, 1 ligne = 1 triplet (pr_debut, pr_fin, code_ligne).\n
    Les durées (et les MTBF) sont calculées avec distinction de la définition des infrastructures concernés par les incidents.
    """

    logging.info("start")
    
    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")

    # import data icd
    df_icd = import_data(inter_folder, "infra_incidents_idf.csv")

    # nettoyage date de début
    df_icd["date_de_debut_min"] = pd.to_datetime(
        df_icd["date_de_debut_min"], format="mixed"
    )

    # dataframes vides pour les concaténations successives des dataframes par infra
    df_durees_infra = pd.DataFrame(data=None, columns=df_icd.columns)
    df_mtbf_infra = pd.DataFrame(data=None, columns=cols_mtbf_infra)

    for infra in df_icd.infra.unique():
        # données d'incidents pour une infra
        df_icd_infra_ = df_icd.loc[df_icd["infra"] == infra, :]

        # calcul des durées pour une infra, avec les info sur l'incident qui a stoppé la durée sans incident
        df_durees_infra_ = process_durations(df_icd_infra_)
        df_durees_infra = pd.concat([df_durees_infra, df_durees_infra_])

        # calcul des mtbf pour une infra
        df_mtbf_infra_ = process_mtbf(df_durees_infra_, **kwargs)
        df_mtbf_infra_.loc[:, "infra"] = infra
        df_mtbf_infra = pd.concat([df_mtbf_infra, df_mtbf_infra_])

    # export data durées et mtbf par infra
    export_data(df_durees_infra, inter_folder, "durations_infra.csv")
    export_data(df_mtbf_infra, output_folder, "mtbf_infra.csv")


if __name__ == "__main__":
    # création des dataframes des durées entre incident et des mtbf toutes infras confondus
    process_tot()

    # création des dataframes des durées entre incident et des mtbf avec distinction par infra
    process_infra()
