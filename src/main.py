import datetime
import os
import logging

from logger import init_logging
from constants import *
from process import process
from merge_mtbf_to_icv import merge_mtbf_to_icv


def process_viz1():
    # statistiques descriptives annuelles sans localisation, mtbf et icd (id par couple de PR), par infra et total
    
    logging.info("start")

    output_folder = "data_output1"
    is_localization_simplified = False

    process(
        output_folder=output_folder,
        is_localization_simplified=is_localization_simplified,
        dates_itv=[[datetime.date(2013, 1, 1), datetime.date(2024, 1, 1)]],
    )

    os.rename(
        os.path.join(inter_folder, "infra_incidents_idf.csv"),
        os.path.join(output_folder, "infra_incidents_idf.csv"),
    )
    os.remove(os.path.join(output_folder, "mtbf_infra.csv"))

    process(
        output_folder=output_folder,
        is_localization_simplified=is_localization_simplified,
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


def process_viz2():
    # carte annuelle des mtbf (id par PR début), par infra et total
    
    logging.info("start")

    output_folder = "data_output2"

    process(
        output_folder=output_folder,
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

    os.rename(
        os.path.join(inter_folder, "mtbf_tot_itv.csv"),
        os.path.join(output_folder, "mtbf_tot_itv.csv"),
    )


def process_viz3():
    # comparaison en 1 carte, mtbf (id par PR début) par période printemps+été vs autonme+hiver
    
    logging.info("start")

    output_folder = "data_output3"

    process(
        output_folder=output_folder,
        is_localization_simplified=True,
        dates_itv=[
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
            [datetime.date(2023, 9, 23), datetime.date(2024, 12, 22)],  # autonome 2023
        ],
    )

    os.rename(
        os.path.join(inter_folder, "mtbf_tot_itv.csv"),
        os.path.join(output_folder, "mtbf_tot_itv.csv"),
    )


def process_viz4():
    # comparaison annuelle en 2 cartes, icv/age vs mtbf/icd (id par PR début)
    
    logging.info("start")

    output_folder = "data_output4"

    process(
        output_folder=output_folder,
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


def process_viz5():
    # comparaison annuelle en 1 carte, jointure data mtbf (id par couple de PR) sur data icv
    
    logging.info("start")

    output_folder = "data_output5"

    # process(
    #     output_folder=output_folder,
    #     is_localization_simplified=False,
    #     dates_itv=[
    #         [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1)],
    #         [datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)],
    #         [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
    #         [datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)],
    #         [datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)],
    #         [datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)],
    #     ],
    # )

    # merge_mtbf_to_icv(
    #     output_folder=output_folder,
    #     infra="VOIE",
    #     key="ID",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder=output_folder,
    #     infra="ADV",
    #     key="CLE_ADV",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder=output_folder,
    #     infra="CAT",
    #     key="ID",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    # merge_mtbf_to_icv(
    #     output_folder=output_folder,
    #     infra="EALE",
    #     key="ID_ARMEN",
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    # )

    merge_mtbf_to_icv(
        output_folder=output_folder,
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
    # process_viz1()

    # viz2
    # process_viz2()

    # viz3
    # process_viz3()

    # viz4
    # process_viz4()

    # viz5
    # process_viz5()


if __name__ == "__main__":
    main()
