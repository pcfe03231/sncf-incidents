import numpy as np
import pandas as pd
import logging

from constants import *
from import_export import *
from clean import clean_GPS, clean_PR, clean_PK


def join_data_reseau(df_ref, item_reseau_def):
    """Jointure des données d'une caractéristique du réseau aux données de référence PR."""
    
    logging.info("start")
    
    # import data referentiel sur l'item de reseau
    df_ = import_data(input_folder, f"referentiel_{item_reseau_def}.csv")

    # association pk entre pkd et pkf (seulement pour les PR de df_ref qui sont en idf)
    df_merge = pd.merge(df_ref[df_ref["en_idf"] == True], df_, on="code_ligne")
    df_merge = df_merge[
        (df_merge["pk"] >= df_merge["pkd"]) & (df_merge["pk"] <= df_merge["pkf"])
    ]

    # faire un groupby par (pr_ref, code_ligne) pour ne prendre que la première ligne / le premier elt
    # car dans df, le découpage pkd pkf peut se superposer
    df_merge = (
        df_merge.groupby(by=["pr_ref", "code_ligne"], as_index=False, dropna=False)
        .agg(item=pd.NamedAgg(column=item_reseau_def, aggfunc=lambda x: list(x)[0]))
        .rename(columns={"item": item_reseau_def})
    )

    # jointure sur les data de ref
    df_ref = pd.merge(df_ref, df_merge, how="left", on=["pr_ref", "code_ligne"])

    return df_ref


def init_data_ref(**kwargs):
    """
    Créer le dataframe référentiel (PR, PR nettoyé, PK, coordonnées GPS, lignes, indicatrice IDF).
    """
    
    logging.info("start")

    input_folder = get_kwargs(kwargs, "input_folder")
    output_folder = get_kwargs(kwargs, "output_folder")

    # import data referentiel, avec uniquement les colonnes nécessaires
    df_ref = import_data(input_folder, "referentiel_pr.csv", usecols=usecols_ref)

    # mettre en forme coord. gps (latitude, longitude), réduire la précision des coord. gps
    df_ref = pd.concat(
        [
            df_ref,
            pd.DataFrame(
                data=df_ref["geometry"].astype(str).apply(clean_GPS).to_list(),
                columns=["longitude", "latitude"],
            ),
        ],
        axis=1,
    ).drop(columns=["geometry"])
    df_ref[["latitude", "longitude"]] = df_ref[["latitude", "longitude"]].round(10)

    # créer colonne "en_idf" indicatrice (1 si le PR est en IDF)
    df_ref = df_ref.assign(
        en_idf=df_ref["region_sncf"].isin(
            [
                "Paris-Saint-Lazare",
                "Paris-Sud-Est",
                "Paris-Est",
                "Paris-Nord",
                "Paris-Rive-Gauche",
            ]
        )
    )

    # supprimer lignes dupliquées et lignes avec au moins une valeur manquante
    df_ref = df_ref.drop_duplicates(ignore_index=True).dropna()

    # renommer colonnes pr et pk, nettoyer pr (pr_ref : libellés PR data initiale ; pr_ : libellés PR nettoyés pour appariement avec les données d'icd)
    df_ref.rename(
        columns={"libelle": "pr_ref", "pk_interne": "pk"},
        inplace=True,
    )
    df_ref["pr_"] = clean_PR(df_ref, "pr_ref")

    # nettoyer pk
    df_ref["pk"] = df_ref["pk"].astype(str).apply(clean_PK).astype(int)

    # ajouter un PR non localisable, pour lequel on va ajouter des pr_ venant des données d'icd
    df_ref.loc[len(df_ref)] = [
        "Non localisable",
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
        False,
        "azertyqwerty",
    ]

    # agréger les données selon pr_ref et code_ligne (donne une sous-liste de pk, latitude, longitude), prendre le plus petit pk et les 1ères coord. gps associés à ce couple
    # 1 PR = 1 ou plusieurs code_ligne = 1 PK par code_ligne (le premier est retenu) = 1 coord. GPS par couple (code_ligne, PK)
    df_ref.sort_values(by=["pr_ref", "code_ligne", "pk"], inplace=True)
    df_ref = df_ref.groupby(
        by=["pr_ref", "code_ligne"], as_index=False, dropna=False
    ).agg(
        pr_=pd.NamedAgg(column="pr_", aggfunc="first"),
        pk=pd.NamedAgg(column="pk", aggfunc="min"),
        latitude=pd.NamedAgg(column="latitude", aggfunc="first"),
        longitude=pd.NamedAgg(column="longitude", aggfunc="first"),
        list_en_idf=pd.NamedAgg(column="en_idf", aggfunc=lambda x: list(x)),
    )

    # simplifier les coord. gps : prendre coord. gps du premier plus petit pk (toute ligne confondue)
    # 1 PR = 1 coord. gps, celle associée au plus petit pk quelle que soit la ligne
    for pr in df_ref["pr_ref"][df_ref["pr_ref"] != "Non localisable"].unique():
        mask = df_ref["pr_ref"] == pr
        mask_ = mask & (df_ref.loc[mask, "pk"] == np.nanmin(df_ref.loc[mask, "pk"]))
        df_ref.loc[mask, ["latitude", "longitude"]] = df_ref.loc[
            mask_, ["latitude", "longitude"]
        ].values[0]

    # PR à la fois en IDF et hors IDF (plusieurs lignes) : comptabiliser comme étant en IDF
    df_ref["en_idf"] = df_ref["list_en_idf"].apply(lambda x: max(x))
    df_ref.drop(columns="list_en_idf", inplace=True)

    # jointure des données de réseaux
    for item_reseau_ref in data_reseau_ref:
        df_ref = join_data_reseau(df_ref, item_reseau_ref)

    # exporter data referentiel
    export_data(df_ref, output_folder, "referentiel_pr.csv")


def modify_PR(*args, **kwargs):
    """
    Modifier des informations sur un PR dans les données de référentiels
    """
    
    logging.info("start")
    
    output_folder = get_kwargs(kwargs, "output_folder")

    # import referentiel pr
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # boucle sur les uplets de args
    for arg in args:
        pr_ref = arg.get("pr_ref")

        # modification de "en_idf"
        en_idf = arg.get("en_idf")
        if en_idf is not None:
            df_ref.loc[df_ref["pr_ref"] == pr_ref, "en_idf"] = en_idf

        # modification de "latitude" (spécifier la nouvelle latitude)
        latitude = arg.get("latitude")
        if latitude is not None:
            df_ref.loc[
                (df_ref["pr_ref"] == pr_ref),  # & (df_ref["code_ligne"] == code_ligne),
                "latitude",
            ] = latitude

        # modification de "longitude" (spécifier la nouvelle longitude)
        longitude = arg.get("longitude")
        if longitude is not None:
            df_ref.loc[
                (df_ref["pr_ref"] == pr_ref),  # & (df_ref["code_ligne"] == code_ligne),
                "longitude",
            ] = longitude

        # modification de "code_ligne" (spécifier le nouveau et l'ancien code_ligne)
        code_ligne_new = arg.get("code_ligne_new")
        code_ligne_old = arg.get("code_ligne_old")
        if code_ligne_new and code_ligne_old is not None:
            df_ref.loc[
                (df_ref["pr_ref"] == pr_ref) & (df_ref["code_ligne"] == code_ligne_old),
                "code_ligne",
            ] = code_ligne_new

        # modification de "pk" (spécifier le nouveau pk et le code_ligne existant auquel il est associé)
        pk = arg.get("pk")
        code_ligne = arg.get("code_ligne")
        if pk and code_ligne is not None:
            df_ref.loc[
                (df_ref["pr_ref"] == pr_ref) & (df_ref["code_ligne"] == code_ligne),
                "pk",
            ] = pk

        # modification de "infrapole"
        infrapole = arg.get("infrapole")
        if infrapole is not None:
            df_ref.loc[df_ref["pr_ref"] == pr_ref, "infrapole"] = infrapole

        # modification de "ligne_transilien" (spécifier la nouvelle ligne_transilien et le code_ligne existant auquel il est associé)
        ligne_transilien = arg.get("ligne_transilien")
        code_ligne = arg.get("code_ligne")
        if ligne_transilien and code_ligne is not None:
            df_ref.loc[
                (df_ref["pr_ref"] == pr_ref) & (df_ref["code_ligne"] == code_ligne),
                "ligne_transilien",
            ] = ligne_transilien

    df_ref.drop_duplicates(ignore_index=True, inplace=True)

    # export data referentiel pr
    export_data(df_ref, output_folder, "referentiel_pr.csv")


def add_PR(*args, **kwargs):
    """
    Ajoute des PR aux données de référentiels en se basant sur un PR existant.\n
    Duplique une ligne du dataframe, le seul champ qui change est 'pr_ref'.\n
    L'input est sous la forme [new_PR, as_PR] qui correspondent à 2 PR nettoyés par la fonction clean_PR.\n
    new_PR est un 'pr_ref' des données d'incidents, as_PR est un 'pr_ref' des données de référence.
    """
    
    logging.info("start")

    output_folder = get_kwargs(kwargs, "output_folder")

    # import referentiel pr
    df_ref = import_data(output_folder, "referentiel_pr.csv")

    # boucle sur les couples [new_PR, as_PR] de args
    for couple_PR in args:
        new_PR = couple_PR[0]
        as_PR = couple_PR[1]

        # créer une ligne identique à celle de as_PR (à part pr_ref, valeur remplacée par new_PR), puis la concaténer au reste des données
        line = pd.DataFrame(columns=df_ref.columns)
        line.loc[0, ["pr_ref", "en_idf", "pr_"]] = [
            df_ref.loc[df_ref["pr_"] == as_PR, "pr_ref"].unique()[0],
            df_ref.loc[df_ref["pr_"] == as_PR, "en_idf"].max(),
            new_PR,
        ]
        df_ref = pd.concat([df_ref, line], axis=0)

    df_ref.drop_duplicates(ignore_index=True, inplace=True)

    # export referentiel pr
    export_data(df_ref, output_folder, "referentiel_pr.csv")


def correct_data_ref(**kwargs):
    """Corrections de PR réunis (ajout et modifications)"""
    
    logging.info("start")
    
    output_folder = get_kwargs(kwargs, "output_folder")

    # modifier des infos sur des PR dans les data de référence (PR à modifier et colonnes concernées, voir la fonction pour détail)
    modify_PR(*PR_to_modify, **kwargs)

    # mettre hors idf les PR non liés au réseau au sens des données de densités et d'uic (valeurs manquantes pour au moins l'une de ces colonnes)
    df_ref = import_data(output_folder, "referentiel_pr.csv", usecols=["pr_ref", "en_idf", "densite", "uic"])
    PR_hors_idf = df_ref.loc[
        (df_ref["en_idf"] == True)
        & (pd.isna(df_ref["densite"]) | pd.isna(df_ref["uic"])),
        "pr_ref",
    ].unique()
    PR_hors_idf = tuple([{"pr_ref": pr_ref, "en_idf": False} for pr_ref in PR_hors_idf])
    modify_PR(*PR_hors_idf, **kwargs)

    # ajouter des PR dans les data de référence (PR à ajouter et PR sur lequel il se base)
    add_PR(*PR_to_add, **kwargs)


if __name__ == "__main__":
    # création du dataframe référentiel
    init_data_ref()
    correct_data_ref()
