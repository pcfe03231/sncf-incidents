# %% Création des data referentiel_temps.csv
import pandas as pd
import datetime

# %% data_output2
df = pd.DataFrame(data=list(range(2010, 2026)), columns=["annee"])

# %%
df.to_csv(
    "../data_output2/referentiel_temps.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)


# %% data_output3
df = pd.DataFrame(
    data=[
        [
            datetime.date(2016, 3, 20),
            datetime.date(2016, 9, 22),
            "printemps-été",
            "2016",
        ],
        [
            datetime.date(2016, 9, 22),
            datetime.date(2017, 3, 20),
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
            datetime.date(2024, 12, 22),
            "automne-hiver",
            "2023",
        ],
    ],
    columns=["date_min", "date_max", "saisons", "annees"],
)

# %%
df.to_csv(
    "../data_output3/referentiel_temps.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %%
