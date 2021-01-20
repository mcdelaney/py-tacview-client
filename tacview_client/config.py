"""Shared config settings for the app."""
from asyncio.log import logging
import os
from pathlib import Path


class DatabaseEnvVarConfigError(Exception):
    """Throw this exception when there is no TACVIEW_DATABASE_URL variable found."""

def get_db_dsn() -> str:
    try:
        dsn = os.getenv("TACVIEW_DATABASE_URL")
        return dsn
    except Exception:
        raise DatabaseEnvVarConfigError(
        """
        No tacview database environment variable could be found!

        Please set an environment variable in the following format:
            Name: TACVIEW_DATABASE_URL
            Value: postgresql://{database-ip-address}:5432/{database-name}?user={username}&password={password}

        NOTE: You must replace the parts inside the curly braces with the relevant values.

        To configure:
            Under Windows you can set this from a powershell session by running:
                setx "TACVIEW_DATABASE_URL" "{Value - per above}"

            Under linux, add the following to the last line of your .bashrc or .profile file:
                export TACVIEW_DATABASE_URL="{Value - per above}"

        After doing so, restart your terminal session and try again.
        """)



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
