# création des data referentiel_temps1.csv et referentiel_temps2.csv (viz1, viz2, viz3)
import os
import pandas as pd
import datetime
from src.constants import output_folder1, output_folder2, output_folder3, encoding, sep


# ref temps par année (viz1, viz2, viz3)

df1 = pd.DataFrame(
    data=list(range(2010, 2026)), columns=["annee"]
)  # modifier s'il le faut

df1.to_csv(
    os.path.join(output_folder1, "referentiel_temps1.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)

df1.to_csv(
    os.path.join(output_folder2, "referentiel_annee.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)

df1.to_csv(
    os.path.join(output_folder3, "referentiel_temps.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)


# ref temps par saison (viz1)

df2 = pd.DataFrame(  # modifier s'il le faut
    data=[
        [
            datetime.date(2016, 3, 20),  # début printemps 2016
            datetime.date(2016, 9, 22),  # fin été 2016
            "printemps-été",
            "2016",
        ],
        [
            datetime.date(2016, 9, 22),  # début automne 2016
            datetime.date(2017, 3, 20),  # fin hiver 2017
            "automne-hiver",
            "2016-2017",
        ],
        [
            datetime.date(2017, 3, 20),
            datetime.date(2017, 9, 22),
            "printemps-été",
            "2017",
        ],
        [
            datetime.date(2017, 9, 22),
            datetime.date(2018, 3, 20),
            "automne-hiver",
            "2017-2018",
        ],
        [
            datetime.date(2018, 3, 20),
            datetime.date(2018, 9, 23),
            "printemps-été",
            "2018",
        ],
        [
            datetime.date(2018, 9, 23),
            datetime.date(2019, 3, 20),
            "automne-hiver",
            "2018-2019",
        ],
        [
            datetime.date(2019, 3, 20),
            datetime.date(2019, 9, 23),
            "printemps-été",
            "2019",
        ],
        [
            datetime.date(2019, 9, 23),
            datetime.date(2020, 3, 20),
            "automne-hiver",
            "2019-2020",
        ],
        [
            datetime.date(2020, 3, 20),
            datetime.date(2020, 9, 22),
            "printemps-été",
            "2020",
        ],
        [
            datetime.date(2020, 9, 22),
            datetime.date(2021, 3, 20),
            "automne-hiver",
            "2020-2021",
        ],
        [
            datetime.date(2021, 3, 20),
            datetime.date(2021, 9, 22),
            "printemps-été",
            "2021",
        ],
        [
            datetime.date(2021, 9, 22),
            datetime.date(2022, 3, 20),
            "automne-hiver",
            "2021-2022",
        ],
        [
            datetime.date(2022, 3, 20),
            datetime.date(2022, 9, 23),
            "printemps-été",
            "2022",
        ],
        [
            datetime.date(2022, 9, 23),
            datetime.date(2023, 3, 20),
            "automne-hiver",
            "2022-2023",
        ],
        [
            datetime.date(2023, 3, 20),
            datetime.date(2023, 9, 23),
            "printemps-été",
            "2023",
        ],
        [
            datetime.date(2023, 9, 23),
            datetime.date(2024, 3, 20),
            "automne-hiver",
            "2023-2024",
        ],
        [
            datetime.date(2024, 3, 20),
            datetime.date(2024, 9, 22),
            "printemps-été",
            "2024",
        ],
        [
            datetime.date(2024, 9, 22),
            datetime.date(2025, 3, 20),
            "automne-hiver",
            "2024-2025",
        ],
        [
            datetime.date(2025, 3, 20),
            datetime.date(2024, 9, 23),
            "printemps-été",
            "2025",
        ],
    ],
    columns=["date_min", "date_max", "saisons", "annees"],
)


df2.to_csv(
    os.path.join(output_folder1, "referentiel_temps2.csv"),
    sep=sep,
    encoding=encoding,
    index=False,
    lineterminator="\n",
)
