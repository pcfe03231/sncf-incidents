import unicodedata
import re


def clean_endings(text):
    """Supprime la chaîne '- word' à la fin d'une chaîne."""
    return re.sub(r"\s-\s\w+$", "", text)


def clean_parenthese(text):
    """Supprime les parenthèses et leurs contenus + les espaces indésirables."""
    return re.sub(r"\(|\)", "", text).strip()


def clean_accents(text):
    """Supprime les accents d'une chaîne de caractères."""
    try:
        nfkd_form = unicodedata.normalize("NFKD", text)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    except Exception:
        return text


def clean_lower(text):
    """Met en minuscule une chaîne de caractères."""
    return text.lower()

def clean_special(text):
    """Supprime les - intermediaires, +, /, ., espaces multiples."""
    text = text.replace("-", " ")
    text = text.replace("+", " ")
    text = text.replace("/", "")
    text = re.sub("\s\s+", ' ', text)
    text = re.sub(r'\w+\.\w+', lambda match: match.group().replace('.', ' '), text)
    text = re.sub(r'\.\B|\B\.', '', text)
    return text

def clean_PR(df, col_name_PR):
    """Mise en forme des libellés de PR."""

    return (
        df[col_name_PR]
        .apply(lambda x: clean_endings(x))
        .apply(lambda x: clean_parenthese(x))
        .apply(lambda x: clean_accents(x))
        .apply(lambda x: clean_lower(x))
        .apply(lambda x: clean_special(x))
    )
    
def clean_GPS(text):
    """Mise en forme des coordonnées GPS."""
    text = re.sub(r"POINT \(|\)", "", text).strip()
    coord = text.split(" ")
    coord = [float(c) for c in coord]
    return coord

def clean_PK(text):
    """Mise en forme des PK (suppression des '00')."""
    return re.sub(r"00", "", text, count=1)


if __name__ == "__main__":
    0
    # df["pr_debut"] = df["pr_debut"].apply(lambda x: clean_accents(x))
    # df["pr_fin"] = df["pr_fin"].apply(lambda x: clean_accents(x))
    # df_ref["PR"] = df_ref["PR"].apply(lambda x: clean_accents(x))
