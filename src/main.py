import datetime
import os
import logging

from logger import init_logging
from constants import *
from process import process
from merge_mtbf_to_icv import merge_mtbf_to_icv


def process_viz1():
    """Carte annuelle totale et cartes semestrielles (2 saisons vs 2 saison) par infra des mtbf."""
    logging.info("start")

    # onglet 1 : une carte annuelle pour toutes les infras confondues
    process(
        output_folder=output_folder1,
        is_localization_simplified=True,
        dates_itv=(
            [datetime.date(2013, 1, 1), datetime.date(2014, 1, 1)],
            [datetime.date(2014, 1, 1), datetime.date(2015, 1, 1)],
            [datetime.date(2015, 1, 1), datetime.date(2016, 1, 1)],
            [datetime.date(2016, 1, 1), datetime.date(2017, 1, 1)],
            [datetime.date(2017, 1, 1), datetime.date(2018, 1, 1)],
            [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)],
            [datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)],
            [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            [datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)],
            [datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)],
            [datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)],
        ),
    )
    os.rename(
        os.path.join(output_folder1, "mtbf_tot_itv.csv"),
        os.path.join(output_folder1, "temp_mtbf_tot_itv_annee.csv"),
    )
    os.rename(
        os.path.join(output_folder1, "mtbf_infra_itv.csv"),
        os.path.join(output_folder1, "temp_mtbf_infra_itv_annee.csv"),
    )

    # onglet 2 : deux cartes semestrielles (printemps+été ou automne+hiver) pour un infra à choisir
    process(
        output_folder=output_folder1,
        is_localization_simplified=True,
        dates_itv=(
            [
                datetime.date(2016, 3, 20),
                datetime.date(2016, 9, 22),
            ],  # printemps été 2016
            [
                datetime.date(2016, 9, 22),
                datetime.date(2017, 3, 20),
            ],  # autonome hiver 2016 2017
            [
                datetime.date(2017, 3, 20),
                datetime.date(2017, 9, 22),
            ],  # printemps été 2017
            [
                datetime.date(2017, 9, 22),
                datetime.date(2018, 3, 20),
            ],  # autonome hiver 2017 2018
            [
                datetime.date(2018, 3, 20),
                datetime.date(2018, 9, 23),
            ],  # printemps été 2018
            [
                datetime.date(2018, 9, 23),
                datetime.date(2019, 3, 20),
            ],  # autonome hiver 2018 2019
            [
                datetime.date(2019, 3, 20),
                datetime.date(2019, 9, 23),
            ],  # printemps été 2019
            [
                datetime.date(2019, 9, 23),
                datetime.date(2020, 3, 20),
            ],  # autonome hiver 2019 2020
            [
                datetime.date(2020, 3, 20),
                datetime.date(2020, 9, 22),
            ],  # printemps été 2020
            [
                datetime.date(2020, 9, 22),
                datetime.date(2021, 3, 20),
            ],  # autonome hiver 2020 2021
            [
                datetime.date(2021, 3, 20),
                datetime.date(2021, 9, 22),
            ],  # printemps été 2021
            [
                datetime.date(2021, 9, 22),
                datetime.date(2022, 3, 20),
            ],  # autonome hiver 2021 2022
            [
                datetime.date(2022, 3, 20),
                datetime.date(2022, 9, 23),
            ],  # printemps été 2022
            [
                datetime.date(2022, 9, 23),
                datetime.date(2023, 3, 20),
            ],  # autonome hiver 2022 2023
            [
                datetime.date(2023, 3, 20),
                datetime.date(2023, 9, 23),
            ],  # printemps été 2023
            [
                datetime.date(2023, 9, 23),
                datetime.date(2024, 12, 22),
            ],  # autonome 2023
        ),
    )
    os.remove(
        os.path.join(output_folder1, "mtbf_tot_itv.csv"),
    )
    os.rename(
        os.path.join(output_folder1, "mtbf_infra_itv.csv"),
        os.path.join(output_folder1, "mtbf_infra_itv_saison.csv"),
    )
    
    os.rename(
        os.path.join(output_folder1, "temp_mtbf_tot_itv_annee.csv"),
        os.path.join(output_folder1, "mtbf_tot_itv_annee.csv"),
    )
    os.rename(
        os.path.join(output_folder1, "temp_mtbf_infra_itv_annee.csv"),
        os.path.join(output_folder1, "mtbf_infra_itv_annee.csv"),
    )


def process_viz2():
    """Comparaison annuelle en 2 cartes, icv/age vs mtbf/icd (localisation par PR de début)."""

    logging.info("start")

    process(
        output_folder=output_folder2,
        is_localization_simplified=True,
        dates_itv=[
            [datetime.date(2013, 1, 1), datetime.date(2014, 1, 1)],
            [datetime.date(2014, 1, 1), datetime.date(2015, 1, 1)],
            [datetime.date(2015, 1, 1), datetime.date(2016, 1, 1)],
            [datetime.date(2016, 1, 1), datetime.date(2017, 1, 1)],
            [datetime.date(2017, 1, 1), datetime.date(2018, 1, 1)],
            [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)],
            [datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)],
            [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            [datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)],
            [datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)],
            [datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)],
        ],
    )


def process_viz3():
    """Comparaison annuelle en 1 carte, jointure des data mtbf (localisation par couple de PR) sur les data icv."""

    logging.info("start")

    process(
        output_folder=output_folder3,
        is_localization_simplified=False,
        dates_itv=[
            [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)],
            [datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)],
            [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            [datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)],
            [datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)],
            [datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)],
        ],
    )

    merge_mtbf_to_icv(
        output_folder=output_folder3,
        infra="VOIE",
        key="ID",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )

    merge_mtbf_to_icv(
        output_folder=output_folder3,
        infra="ADV",
        key="CLE_ADV",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )

    merge_mtbf_to_icv(
        output_folder=output_folder3,
        infra="CAT",
        key="ID",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )

    merge_mtbf_to_icv(
        output_folder=output_folder3,
        infra="EALE",
        key="ID_ARMEN",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )

    merge_mtbf_to_icv(
        output_folder=output_folder3,
        infra="SIG",
        key="ID",
        years=[2018, 2019, 2020, 2021, 2022, 2023],
    )


def main():
    init_logging()
    logging.info("start")

    # par defaut
    process()

    # viz1
    process_viz1()

    # viz2
    process_viz2()

    # viz3
    process_viz3()


if __name__ == "__main__":
    main()
