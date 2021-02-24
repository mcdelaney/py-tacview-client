import asyncio
from functools import partial
from multiprocessing import Process
from pathlib import Path
import sys

import typer  # type: ignore

from tacview_client import client, serve_file, config

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
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="""If true, and the file specified has been processed previously,
                all results will be dropped and the file will be re-processed.
    """,
    ),
    debug: bool = typer.Option(False, "--debug", hidden=True),
):
    """Interface to the tacview batch/single file processer."""
    LOG.info(f"Starting client...")
    if not filename:
        LOG.error("Filename must be specified!")
        sys.exit(1)

    if filename and not filename.exists():
        LOG.erro(f"File does not exist at location: {filename}")
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
            client_username="tacview-client",
            client_password="0",
            debug=debug,
            max_iters=None,
            batch_size=batch_size,
            overwrite=overwrite,
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
    client_name: str = typer.Option(
        "tacview-client",
        "--client_name",
        help="""Client username that we should use to authenticate to the remote tacview server.""",
    ),
    client_password: str = typer.Option(
        "0",
        "--client_password",
        help="""Client password that we should use to authenticate to the remote tacview server.""",
    ),
    debug: bool = typer.Option(False, "--debug", hidden=True),
):
    """Interface to the tacview stream processer."""
    LOG.info(f"Starting client in stream mode...")
    try:
        client.main(
            host=host,
            port=port,
            client_username=client_name,
            client_password=client_password,
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
    from tacview_client import db
    LOG.info("Dropping tables from specified database...")
    async def drop_and_recreate():
        await db.drop_tables()
        await db.create_tables()

    asyncio.run(drop_and_recreate())
    LOG.info("Recreating tables in specified database...")


@app.command("createdb")
def createdb():
    """Create database tables."""
    from tacview_client import db
    LOG.info("Creating tables in specified database...")
    asyncio.run(db.create_tables())
