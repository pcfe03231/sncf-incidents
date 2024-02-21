import os
import logging
from datetime import date
from constants import logs_folder


def init_logging():
    """Initialisation des logs."""

    format = "%(asctime)s | %(levelname)s | %(filename)s - %(funcName)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        format=format,
        datefmt=date_format,
        level=logging.DEBUG,
    )

    log_file_name = f"{date.today().strftime('%Y-%m-%d')}.log"
    handler = logging.FileHandler(os.path.join(logs_folder, log_file_name))

    formatter = logging.Formatter(fmt=format, datefmt=date_format)
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(handler)
