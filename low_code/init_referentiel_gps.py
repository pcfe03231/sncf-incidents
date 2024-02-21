# %% stocker toutes les coordonnées (longitude, latitude) uniques
import pandas as pd
import os

#viz3
# %%
df_gps = pd.DataFrame(columns=["X", "Y"])

# %%
df_ref = pd.read_csv(
    os.path.join("../data_output3", "referentiel_pr.csv"),
    sep="\t",
    encoding="utf-16",
    low_memory=False,
    usecols=["longitude", "latitude"]
)

df_gps[["X", "Y"]] = df_ref[["longitude", "latitude"]].drop_duplicates(ignore_index=True)

# %%
df_gps.to_csv(
    os.path.join("../data_output3", "referentiel_gps.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

#viz4
# %%
icv_files = [
    "sig_icv.csv",
    "voie_icv.csv",
    "eale_icv.csv",
    "adv_icv.csv",
    "cat_icv.csv",
]

for icv_file in icv_files:
    df = pd.read_csv(
        os.path.join("../data_input_icv", icv_file),
        sep="\t",
        encoding="utf-16",
        low_memory=False,
        usecols=["X", "Y"],
    )

    df_gps = pd.concat([df_gps, df])
    df_gps.drop_duplicates(inplace=True, ignore_index=True)

# %%
df_gps.to_csv(
    os.path.join("../data_output4", "referentiel_gps.csv"),
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %%

# %%
