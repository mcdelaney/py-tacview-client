"""Shared config settings for the app."""
from asyncio.log import logging
import os
from pathlib import Path

DB_URL = os.getenv(
    "TACVIEW_DATABASE_URL", "postgresql://0.0.0.0:5432/dcs?user=prod&password=pwd"
)


def get_logger(logger_name="tacview_client") -> logging.Logger:
    """Create a package logger for tacview-client."""
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    logFormatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s"
    )
    file_path = Path(f"log/{log.name}.log")
    if not file_path.parent.exists():
        file_path.parent.mkdir()
    if log.hasHandlers():
        return log

    fileHandler = logging.FileHandler(file_path, "w")
    fileHandler.setFormatter(logFormatter)
    log.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    log.addHandler(consoleHandler)
    log.propagate = False
    return log
