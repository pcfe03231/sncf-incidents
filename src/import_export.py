import os
import pandas as pd

from constants import sep, encoding

#
def import_data(folder, file, **kwargs):
    """Lecture des données dans folder/file."""
    return pd.read_csv(
        os.path.join(folder, file),
        sep=sep,
        encoding=encoding,
        **kwargs
    )
    
def export_data(df, folder, file):
    """Ecriture des données df dans folder/file."""
    df.to_csv(
        os.path.join(folder, file),
        sep=sep,
        encoding=encoding,
        index=False,
        lineterminator="\n",
    )