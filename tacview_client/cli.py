import argparse
from functools import partial
from multiprocessing import Process
from pathlib import Path
import sys

import typer  # type: ignore

from tacview_client import client, serve_file, db, config

LOG = config.get_logger()
app = typer.Typer()


@app.command("process_file")
def process_file(
    filename: Path = typer.Option(
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
    """Interface to the tacview batch/single file processer."""
    LOG.info(f"Starting client...")
    if filename and not filename.exists():
        LOG.info(f"File does not exist at location: {filename}")
        sys.exit(1)

    try:
        LOG.info("Processing acmi file in batch mode...")
        server_proc = Process(
            target=partial(serve_file.main, filename=filename, port=42674)
        )
        server_proc.start()
        client.main(
            host="localhost",
            port=42674,
            debug=debug,
            max_iters=None,
            batch_size=batch_size,
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


@app.command("process_stream")
def process_stream(
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
        42674,
        "--port",
        help="""Port where the existing tacview stream is available.
        Tacview sets this to 42674 by default.
        Unless the server to which you are connecting has manually edited the tacview port,
        you should not need to modify this parameter.""",
    ),
    debug: bool = typer.Option(False, "--debug", hidden=True),
):
    """Interface to the tacview stream processer."""
    LOG.info(f"Starting client in stream mode...")
    try:
        client.main(
            host=host,
            port=port,
            debug=debug,
            max_iters=None,
            batch_size=500,
        )
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
def dropdb():
    """Drop database tables."""
    LOG.info("Dropping tables from specified database...")
    db.drop_tables()


@app.command("createdb")
def createdb():
    """Create database tables."""
    LOG.info("Creating tables in specified database...")
    db.create_tables()
