import datetime

# encodage et séparateur pour le stockage des données en csv
encoding = "utf-16"
sep = "\t"


# fonction permettant de récupérer la valeur associée à la clef (key) dans le dictionnaire kwargs
def get_kwargs(kwargs, key):
    """Fonction permettant de récupérer la valeur associée à la clef key dans le dictionnaire kwargs"""
    if kwargs.get(key) is not None:
        return kwargs.get(key)
    else:
        return eval(key)


# dossiers qui regroupent les données et les logs (traitements par défaut)
input_folder = "data_input"  # dossier des données brutes
inter_folder = "data_inter"  # dossier des données intermédiaires
output_folder = "data_output"  # dossier des données finales
input_icv_folder = "data_input_icv"  # dossier des données brutes
logs_folder = "logs"  # dossier des logs

# traitements simplifiés par PR de début (True) ou traitements standards par couple de PR (False, traitements par défaut)
is_localization_simplified = False

# intervalles de temps où l'on considère les incidents à traiter séparemment (traitements par défaut)
# la journée du min est inclue dans l'intervalle, la journée du max est exclue de l'intervalle
# borne inférieure : 1er janvier 2010 ; format : année, mois, jour
date_min = datetime.date(2023, 1, 1)
date_max = datetime.date(2024, 1, 1)
dates_itv = [[date_min, date_max]]


# noms d'item decrivant le réseau, permet de retrouver les données de référence sur le réseau dans input_folder
data_reseau_ref = [
    "densite",
    "infrapole",
    "ligne_transilien",
    "uic",
]

# colonnes requises dans les données d'incidents (incidents_*) dans input_folder
usecols_icd = [
    "IO - Numéro d'incident",
    "IO - Date de début",
    "EC - Heure",
    "IO - Date de fin",
    "IO - Type d'incident",
    "IO - Type de défaillance",
    "IO - Type de ressource défaillante",
    "IO - Libellé PR (PR début)",
    "IO - Libellé PR (PR fin)",
    "Minutes perdues",
    "Nbre trains impactés",
    "Nbre trains suppr",
]

# colonnes requises dans les données de référence pr (referentiel_pr) dans input_folder
usecols_ref = [
    "libelle",
    "region_sncf",
    "code_ligne",
    "pk_interne",
    "geometry",
]

# colonnes dans les données d'incidents pour le preprocessing par année, lors de l'initialisation des données d'incidents (données intermédiaires)
cols_icd_preprocess = [
    "numero",
    "date_de_debut_min",
    "date_de_fin_max",
    "type_incident",
    "type_def",
    "type_res_def",
    "pr_debut",
    "pr_fin",
    "nb_perturbations_trains",
    "nb_suppressions_trains",
    "nb_minutes_perdues",
]

# colonnes dans les données de référence pr (referentiel_pr) dans output_folder
cols_ref = [
    "pr_ref",
    "code_ligne",
    "pr_",
    "pk",
    "en_idf",
    "latitude",
    "longitude",
]

# colonnes dans les données mtbf totales (mtbf_tot) dans output_folder
cols_mtbf_tot = [
    "pr_debut_ref",
    "pr_fin_ref",
    "liste_code_ligne",
    "itv_min",
    "itv_max",
    "nombre_icd",
    "ratio_itv_sur_nbicd",
    "minimum_tbf",
    "mediane_tbf",
    "maximum_tbf",
    "nombre_perturbations_trains",
    "nombre_suppressions_trains",
    "nombre_minutes_perdues",
]

# colonnes dans les données mtbf par infra (mtbf_infra) dans output_folder
cols_mtbf_infra = [
    "pr_debut_ref",
    "pr_fin_ref",
    "liste_code_ligne",
    "infra",
    "itv_min",
    "itv_max",
    "nombre_icd",
    "ratio_itv_sur_nbicd",
    "minimum_tbf",
    "mediane_tbf",
    "maximum_tbf",
    "nombre_perturbations_trains",
    "nombre_suppressions_trains",
    "nombre_minutes_perdues",
]


# PR à modifier dans les data de référence
PR_to_modify = (
    {"pr_ref": "Mâcon-Loché-TGV", "en_idf": False},
    {"pr_ref": "St-Pierre-des-Corps", "en_idf": False},
    {"pr_ref": "Le Creusot-Montceau-Montchanin", "en_idf": False},
    {"pr_ref": "Le Creusot-Montceau-Montchanin", "code_ligne": 752000, "pk": 27300816},
    {"pr_ref": "Haussmann-St-Lazare", "infrapole": "PSL"},
    {"pr_ref": "Brétigny", "code_ligne": 570316, "ligne_transilien": "Hors Transilien"},
    {"pr_ref": "Paris-Gare-de-Lyon", "latitude": 48.84414217},
    {"pr_ref": "Paris-Gare-de-Lyon", "longitude": 2.37399800},
    {"pr_ref": "Paris-Gare-de-Lyon (Banlieue)", "latitude": 48.84431152},
    {"pr_ref": "Paris-Gare-de-Lyon (Banlieue)", "longitude": 2.37564528},
    {"pr_ref": "Paris-Austerlitz (Banlieue)", "latitude": 48.84149521},
    {"pr_ref": "Paris-Austerlitz (Banlieue)", "longitude": 2.36603764},
    {"pr_ref": "Paris-Nord-Souterraine", "latitude": 48.88064194},
    {"pr_ref": "Paris-Nord-Souterraine", "longitude": 2.35616002},
)


# PR à ajouter dans les data de référence (pr_ à ajouter et pr_ sur lequel il se base)
PR_to_add = (
    ["aerop 1 ch de gaulle", "roissy aeroport cdg 1"],
    ["aerop 2 ch de gaulle tgv", "roissy aeroport cdg 2 tgv"],
    ["paris nord gare banlieue", "paris nord souterraine"],
    ["gare du nord", "paris nord"],
    ["paris aust banlieue", "paris austerlitz banlieue"],
    ["ply banlieue", "paris gare de lyon banlieue"],
    ["courtalain poste 17", "courtalain st pellerin"],
    ["sevres", "sevres ville d'avray"],
    ["sevres r g", "sevres rive gauche"],
    ["villeneuve prairie", "choisy le roi"],
    ["versailles r d", "versailles rive droite"],
    ["versailles rg chateau", "versailles rive gauche"],
    ["lumes voyageurs", "lumes"],
    ["noisy le roi", "noisy le roi t13"],
    ["cannes voyageurs", "cannes"],
    ["nanterre prefecture", "nanterre universite"],
    ["chaville r d", "chaville rive droite"],
    ["chaville r g", "chaville rive gauche"],
    ["viroflay r d", "viroflay rive droite"],
    ["viroflay r g", "viroflay rive gauche"],
    ["paris bercy voyageurs", "paris bercy"],
    ["pince precigne", "azertyqwerty"],
    ["signal km 126,6 v2", "azertyqwerty"],
)
