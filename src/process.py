import os
from glob import glob
import pandas as pd
import logging

from constants import *
from import_export import *
from init_data_ref import init_data_ref, correct_data_ref
from init_data_icd import init_data_icd, correct_data_icd
from process_durations_mtbf import process_tot, process_infra
from matching import matching


def single_process(**kwargs):
    """Traitement unique sur l'intervalle de temps délimité par date_min et date_max"""
    
    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")
    date_min = get_kwargs(kwargs, "dates_itv")[0][0]
    date_max = get_kwargs(kwargs, "dates_itv")[0][1]
    kwargs.update({"date_min": date_min})
    kwargs.update({"date_max": date_max})

    # supprimer les fichiers de données s'ils existent déjà
    if os.path.isfile(os.path.join(inter_folder, "mtbf_tot.csv")):
        os.remove(os.path.join(inter_folder, "mtbf_tot.csv"))

    if os.path.isfile(os.path.join(output_folder, "mtbf_infra.csv")):
        os.remove(os.path.join(output_folder, "mtbf_infra.csv"))

    # création du dataframe référentiel
    init_data_ref(**kwargs)
    correct_data_ref(**kwargs)

    # création du dataframe icd total
    init_data_icd(**kwargs)
    correct_data_icd(**kwargs)
    matching(**kwargs)

    # calcul des durées et des MTBF, sans et avec distinction des définitions d'infrastructure
    process_tot(**kwargs)
    process_infra(**kwargs)


def multi_process(**kwargs):
    """Traitements multiples sur les intervalles de temps délimités par dates_itv"""
    
    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")
    dates_itv = get_kwargs(kwargs, "dates_itv")

    # supprimer les fichiers de données s'ils existent déjà
    if os.path.isfile(os.path.join(inter_folder, "mtbf_tot_itv.csv")):
        os.remove(os.path.join(inter_folder, "mtbf_tot_itv.csv"))

    if os.path.isfile(os.path.join(output_folder, "mtbf_infra_itv.csv")):
        os.remove(os.path.join(output_folder, "mtbf_infra_itv.csv"))

    # création du dataframe référentiel
    init_data_ref(**kwargs)
    correct_data_ref(**kwargs)

    for i, itv in enumerate(dates_itv, 1):
        kwargs.update({"date_min": itv[0]})
        kwargs.update({"date_max": itv[1]})

        # création du dataframe icd total
        init_data_icd(**kwargs)
        correct_data_icd(**kwargs)
        matching(**kwargs)

        # calcul des durées et des MTBF, sans et avec distinction des définitions d'infrastructure
        process_tot(**kwargs)
        process_infra(**kwargs)

        # renommage des fichiers de données mtbf
        os.rename(
            os.path.join(inter_folder, f"mtbf_tot.csv"),
            os.path.join(inter_folder, f"mtbf_tot_itv{i}.csv"),
        )
        os.rename(
            os.path.join(output_folder, f"mtbf_infra.csv"),
            os.path.join(output_folder, f"mtbf_infra_itv{i}.csv"),
        )


def concat_data(**kwargs):
    """
    Dans le cas où les traitement sont réalisés sur plusieurs intervalles de temps, concaténation en seul fichier des données de sortie.
    """
    
    logging.info("start")

    inter_folder = get_kwargs(kwargs, "inter_folder")
    output_folder = get_kwargs(kwargs, "output_folder")

    # récupérer les noms de fichiers par intervalle de temps dans output_folder et créer les df vides
    tot_files = sorted(glob(os.path.join(inter_folder, "mtbf_tot_itv*.csv")))
    infra_files = sorted(glob(os.path.join(output_folder, "mtbf_infra_itv*.csv")))
    df_tot = pd.DataFrame(None, columns=cols_mtbf_tot)
    df_infra = pd.DataFrame(None, columns=cols_mtbf_infra)

    for tot_file, infra_file in zip(tot_files, infra_files):
        # importer le csv tot pour un intervalle de temps
        df_tot_ = import_data(*os.path.split(tot_file))

        # concaténer avec le df mtbf total qui réunit tous les intervalles
        df_tot = pd.concat([df_tot, df_tot_])

        # importer le csv par infra pour un intervalle de temps
        df_infra_ = import_data(*os.path.split(infra_file))

        # concaténer avec le df mtbf par infra qui réunit tous les intervalles
        df_infra = pd.concat([df_infra, df_infra_])

        # supprimer le fichier associé à un seul intervalle de temps
        os.remove(tot_file)
        os.remove(infra_file)

    # exporter df tot
    export_data(df_tot, inter_folder, "mtbf_tot_itv.csv")

    # exporter df infra
    export_data(df_infra, output_folder, "mtbf_infra_itv.csv")


def process(**kwargs):
    """Traitement unique (sur un seul intervalle de temps) ou multiple (sur plusieurs intervalles de temps)"""
    
    logging.info("start")

    dates_itv = get_kwargs(kwargs, "dates_itv")

    # traitement sur un seul intervalle, délimité par date_min et date_max dans constants
    if len(dates_itv) == 1:
        single_process(**kwargs)

    # traitement sur plusieurs intervalles, délimités par dates_itv dans constants
    if len(dates_itv) > 1:
        multi_process(**kwargs)
        concat_data(**kwargs)


if __name__ == "__main__":
    process()
