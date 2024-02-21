# %% Mettre les data icv dans le format choisi (tabulation comme séparateur, utf-16 comme encodage), suppression critere et valeurs
# non automatique
import os
import pandas as pd

# %% import data sig_icv d'origine (avec Critères et Valeur)
df = pd.read_csv(
    "../data_input_icv/sig_icv_.csv",
    sep=";",
    encoding="latin-1",
    low_memory=False,
)

# %% nombre de ligne (dataframe en sortie aura n/5 lignes)
n = df.shape[0]

# %% rangement des lignes selon la colonne Critères/CRITERES, ajout d'une colonne i pour que chaque ligne soit unique
df_ = df.sort_values(by="Critères").assign(i=list(range(df.shape[0])))

# %% pivotage (suppression des colonnes déjà existantes dans Critères/CRITERES)
df_ = df_.pivot(
    index=[
        "i",
        "ID",
        # "LIGNE",
        "LIB_LIGNE",
        "TRL",
        "PK",
        "LIGNE_BIS",
        "PK_BIS",
        "NOM",
        "NOM_ANCIEN",
        "LIBELLÉ_CTV",
        "LIBELLÉ_TYPE_CTV",
        "TYPE_PG",
        "DÉTENTEUR",
        "ETAT",
        "BATIMENT",
        "LOCAL",
        "NOM_DE_CHASSIS",
        "NOM_NUM_COLONNE",
        "TEXTE",
        "REPÈRE",
        "ETAGE",
        "SAF",
        "SECTEUR",
        "DÉCOMPTE",
        "UIC_MAX",
        "VITESSE_MAX",
        "ELEC",
        "STATUT",
        "UIC",
        "GR_UIC_init",
        "ANNEE",
        "NB_AS_GLOBAL",
        "AGE_MOYEN_AS_GLOBAL",
        "VOIE",
        # "Gr_UIC",
        "DZP",
        # "IPOLE",
        "UP",
        "ID:1",
        "X",
        "Y",
        "LIB_LIGNE_BASE",
        "ID_GLOBAL",
    ],
    columns="Critères",
    values="Valeur",
)

# %% reset des indices et drop colonne i
df_ = df_.reset_index().drop(["i"], axis=1)

# %%
df__ = df_.loc[0:(n/5-1)]
df__.loc[:,"Gr_UIC"] = df_.loc[(n/5):(2*n/5-1), "Gr_UIC"].values
df__.loc[:,"IPOLE"] = df_.loc[(2*n/5):(3*n/5-1), "IPOLE"].values
df__.loc[:,"LIBELLE_LIGNE"] = df_.loc[(3*n/5):(4*n/5-1), "LIBELLE_LIGNE"].values
df__.loc[:,"LIGNE_TRANSILIEN"] = df_.loc[(4*n/5):(n-1), "LIGNE_TRANSILIEN"].values



# %%
df__.to_csv(
    "../data_input_icv/cat_icv__.csv",
    sep="\t",
    encoding="utf-16",
    index=False,
    lineterminator="\n",
)

# %% drop le fichier icv 
os.remove("../data_input_icv/sig_icv_.csv")

