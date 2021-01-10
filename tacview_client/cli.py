import argparse
from functools import partial
from multiprocessing import Process
from pathlib import Path
import sys

import typer  # type: ignore

from tacview_client import client, serve_file, db, config

LOG = config.get_logger()
app = typer.Typer()

pg_option = typer.Option(
    ...,
    "--postgres_dsn",
    help="""DSN for connection to the postgres server. Format should be:\n
    postgresql://{ip}:{port}/{dbname}?user={username}&password={password}""",
)


@app.command("run")
def tacview(
    postgres_dsn: str = pg_option,
    host: str = typer.Option(
        ...,
        "--host",
        help="""The IP address where the tacview team is being served.
        If set to set either "127.0.0.1", "localhost" or "0.0.0.0"
        and the filename argument is set a second process will be
        created serve the file.
        """,
    ),
    port: int = typer.Option(
        ...,
        "--port",
        help="""Port where the existing tacview stream is available.
        Tacview sets this to 42674 by default.
        Unless the server to which you are connecting has manually edited the tacview port,
        you should not need to modify this parameter.""",
    ),
    filename: str = typer.Option(
        None,
        "--filename",
        help="""Path to valid tacview acmi file that should be read.
    This is only used if host = 127.0.0.1 or localhost.
    """,
    ),
    batch_size: int = typer.Option(
        500000,
        "--batch_size",
        help="""Number of records to be combined in write batches.
    Under normal usage, this variable should not be modified.
    """,
    ),
    debug: bool = typer.Option(False, "--debug", hidden=True),
):
    """Main interface to client reader."""
    LOG.info(f"Starting client...")
    if filename and not filename.exists():
        LOG.info(f"File does not exist at location: {filename}")
        sys.exit(1)

    try:
        if host in ["localhost", "127.0.0.1", "0.0.0.0"] and filename:
            LOG.info(
                "Localhost and filename configured...will start server to host file..."
            )
            server_proc = Process(
                target=partial(serve_file.main, filename=filename, port=port)
            )
            server_proc.start()
            client.main(
                host=host,
                port=port,
                debug=debug,
                max_iters=None,
                batch_size=batch_size,
                dsn=postgres_dsn,
            )
            server_proc.terminate()
    except KeyboardInterrupt:
        LOG.info("tacview-client shutting down...")
    except Exception as err:
        LOG.error(err)
    finally:
        try:
            server_proc.terminate()  # type: ignore
        except Exception:
            pass


@app.command("dropdb")
def dropdb(postgres_dsn: str = pg_option):
    """Drop database tables."""
    LOG.info("Dropping tables from specified database...")
    db.drop_tables()


@app.command("createdb")
def createdb(postgres_dsn: str = pg_option):
    """Create database tables."""
    LOG.info("Creating tables in specified database...")
    db.create_tables()
