# %% création des data referentiel_temps1.csv et referentiel_temps2.csv (viz1)
import pandas as pd
import datetime

# %% ref temps par année
df1 = pd.DataFrame(data=list(range(2010, 2026)), columns=["annee"])

# %%
df1.to_csv(
    "../data_output1/referentiel_temps1.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %%
df1.to_csv(
    "../data_output3/referentiel_temps.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %% ref temps par saison
df2 = pd.DataFrame(
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
            datetime.date(2023, 12, 22),
            "automne-hiver",
            "2023",
        ],
    ],
    columns=["date_min", "date_max", "saisons", "annees"],
)

# %%
df2.to_csv(
    "../data_output1/referentiel_temps2.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %%
