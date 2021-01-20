"""
Tacview client methods.

Results are parsed into usable format, and then written to a postgres database.
"""
import asyncio
from asyncio.streams import IncompleteReadError
from functools import partial
import logging
from multiprocessing import Process
import time
from typing import Optional
from pathlib import Path
import sys
import os

import asyncpg


try:
    import uvloop  # type: ignore

    uvloop.install()
except (ModuleNotFoundError, NotImplementedError):
    pass

from tacview_client.config import get_db_dsn, get_logger
from tacview_client.copy_writer import BinCopyWriter
from tacview_client import cython_funs as cyfuns

from tacview_client import serve_file
from tacview_client import __version__


DB_URL = get_db_dsn()

STREAM_PROTOCOL = "XtraLib.Stream.0"
TACVIEW_PROTOCOL = "Tacview.RealTimeTelemetry.0"
HANDSHAKE_TERMINATOR = "\0"


HOST = "147.135.8.169"  # Hoggit Gaw
PORT = 42674
DEBUG = False

LOG = get_logger()


class EndOfFileException(Exception):
    """Throw this exception when the server sends a null string,
    indicating end of file.."""


class MaxIterationsException(Exception):
    """Throw this exception when max iters < total_iters."""



async def write_ref_values(ref, overwrite):
    if ref.all_refs and not ref.session_id:
        async with ASYNC_CON.acquire() as con:
            if overwrite:
                await con.execute(
                    f"""DELETE FROM session
                    WHERE start_time = '{ref.start_time}'
                    """)

            LOG.info("All Refs found...writing session data to db...")
            sql = """INSERT into session (lat, lon, title,
                            datasource, author, file_version, start_time,
                            client_version, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING session_id
            """

            ref.session_id = await con.fetchval(
                sql,
                ref.lat,
                ref.lon,
                ref.title,
                ref.datasource,
                ref.author,
                ref.file_version,
                ref.start_time,
                ref.client_version,
                ref.status,
            )
            LOG.info(f"Creating table partion for {ref.session_id}...")
            async with ASYNC_CON.acquire() as con:
                await con.execute(
                    f"""CREATE TABLE event_{ref.session_id} PARTITION OF event
                        FOR VALUES IN ({ref.session_id});
                    """
                )

        LOG.info("Session session data saved...")
        return ref


class AsyncStreamReader:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    """Read from Tacview socket."""

    def __init__(
        self,
        host,
        port,
        client: str = "tacview-client",
        password: str = "0",
        debug: bool = False,
    ):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.sink = "log/raw_sink.txt"
        self.client: str = client
        self.password: str = password
        self.debug = debug
        if self.debug:
            open(self.sink, "w").close()

    async def open_connection(self):
        """
        Initialize the socket connection and write handshake data.
        If connection fails, wait 3 seconds and retry.
        """
        while True:
            try:
                LOG.info(f"Opening connection to {self.host}:{self.port}...")
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port
                )
                LOG.info("Connection opened...sending handshake...")
                HANDSHAKE = (
                    "\n".join(
                        [STREAM_PROTOCOL, TACVIEW_PROTOCOL, self.client, self.password]
                    )
                    + HANDSHAKE_TERMINATOR
                ).encode("utf-8")

                self.writer.write(HANDSHAKE)
                await self.reader.readline()
                LOG.info("Connection opened with successful handshake...")
                break
            except ConnectionError:
                LOG.error("Connection attempt failed....retrying in 3 sec...")
                await asyncio.sleep(3)

    async def read_stream(self):
        """Read lines from socket stream."""
        # if self.reader.at_eof():
        #     raise EndOfFileException
        try:
            data = await self.reader.readuntil(b"\n")
        except IncompleteReadError:
            raise EndOfFileException
        if not data:
            raise EndOfFileException
        return data[:-1].decode("UTF-8")

    async def close(self, status, session_id):
        """Close the socket connection and reset ref object."""
        self.writer.close()
        await self.writer.wait_closed()
        LOG.info(f"Marking session status: {status}...")
        await ASYNC_CON.execute(
            """
                UPDATE session SET status = $1
                WHERE session_id = $2
            """,
            status,
            session_id,
        )
        LOG.info(f"Session marked as {status}!")


async def consumer(
    host: str,
    port: int,
    client_username: str,
    client_password: str,
    max_iters: Optional[int],
    overwrite: bool,
    batch_size: int,
) -> None:
    """Main method to consume stream."""
    LOG.info(
        "Starting tacview client with settings: " "debug: %s -- batch-size: %s",
        DEBUG,
        batch_size,
    )
    dsn = os.getenv("TACVIEW_DATABASE_URL")
    global ASYNC_CON
    ASYNC_CON = await asyncpg.create_pool(DB_URL)
    sock = AsyncStreamReader(
        host,
        port,
        client_username,
        client_password,
    )
    ref = cyfuns.Ref()
    copy_writer = BinCopyWriter(dsn, batch_size, ref=ref)
    await copy_writer.setup()
    await sock.open_connection()
    init_time = time.time()
    lines_read = 0
    last_log = float(0.0)
    print_log = float(0.0)
    line_proc_time = float(0.0)
    try:
        while True:
            # Loop until ref header is read.
            obj = await sock.read_stream()
            LOG.debug(obj)
            lines_read += 1

            if not ref.all_refs:
                ref.parse_ref_obj(obj)
                continue

            ref = await write_ref_values(ref, overwrite)
            copy_writer.session_id = ref.session_id
            break  # All refs have been collected. Break to main loop.

        while True:

            obj = await sock.read_stream()
            LOG.debug(obj)
            lines_read += 1

            if obj[0:1] == "#":
                ref.update_time(obj)
                await copy_writer.insert_data_maybe()

                runtime = time.time() - init_time
                log_check = runtime - last_log
                print_check = runtime - print_log
                if log_check > 0.05:
                    ln_sec = lines_read / runtime
                    sys.stdout.write(
                        "\rEvents processed: {:,} at {:,.2f} events/sec".format(
                            lines_read, ln_sec
                        )
                    )
                    sys.stdout.flush()
                    last_log = runtime

                    if print_check > 10:
                        LOG.info(
                            "Events processed: {:,} at {:,.2f} events/sec".format(
                                lines_read, ln_sec
                            )
                        )
                        print_log = runtime
            else:
                t1 = time.time()
                obj, found_impact = cyfuns.proc_line(obj, ref)
                line_proc_time += time.time() - t1
                if not obj:
                    continue

                if not obj:
                    continue

                if found_impact:
                    copy_writer.add_impact(obj)

                if not obj.written:
                    await copy_writer.create_single(obj)

                copy_writer.add_data(obj)

            if max_iters and max_iters < lines_read:
                copy_writer.session_id = ref.session_id
                await copy_writer.insert_data_maybe()
                LOG.info(f"Max iters reached: {max_iters}...returning...")
                raise MaxIterationsException

    except (
        MaxIterationsException,
        EndOfFileException,
    ) as err:
        LOG.info(f"Starting shutdown due to: {err.__class__.__name__}")
        LOG.info(f"Don't worry, this is expected behavior...")
        await copy_writer.cleanup()
        await sock.close(status="Success",session_id= ref.session_id)

        total_time = time.time() - init_time
        LOG.info("Total Lines Processed: %s", str(lines_read))
        LOG.info("Total seconds running: %.2f", total_time)
        LOG.info("Total db write time: : %.2f", copy_writer.db_event_time)
        LOG.info(
            "Pct Event Write Time: %.2f", copy_writer.db_event_time / total_time
        )
        LOG.info("Pct Line Proc Time: %.2f", line_proc_time / total_time)
        LOG.info("Total Line Proc Secs: %.2f", line_proc_time)
        LOG.info("Lines Proc Per Sec: %.2f", lines_read / line_proc_time)
        LOG.info("Total Lines/second: %.4f", lines_read / total_time)
        total = {}
        for obj in ref.obj_store.values():
            if obj.should_have_parent and not obj.parent:
                try:
                    total[obj.Type] += 1
                except KeyError:
                    total[obj.Type] = 1
        for key, value in total.items():
            LOG.info(f"Total events without parent but should {key}: {value}")
        await check_results()
        LOG.info("Exiting tacview-client!")
        return

    except asyncpg.UniqueViolationError as err:
        LOG.error(
            "The file you are trying to process is already in the database! "
            "To re-process it, delete all associated rows."
        )
        await sock.close(status="Error")
        raise err

    except Exception as err:
        await sock.close(status="Error", session_id=ref.session_id)

        LOG.error(
            "Unhandled Exception!" "Writing remaining updates to db and exiting!"
        )
        LOG.error(err)
        raise err


async def check_results():
    """Collect summary statistics on object and event records."""
    con = await asyncpg.connect(DB_URL)
    result = await con.fetchrow(
        """SELECT COUNT(*) objects, COUNT(parent) parents,
                (SELECT COUNT(*) FROM impact) impacts,
                MAX(updates) max_upate, SUM(updates) total_updates,
                (SELECT COUNT(*) events FROM event) as total_events,
                COUNT(CASE WHEN alive THEN 1 END) total_alive
                FROM object"""
    )

    LOG.info(
        "Results:\n\tobjects: {} \n\tparents: {} \n\timpacts: {}"
        "\n\tmax_updates: {} \n\ttotal updates: {}"
        "\n\ttotal events: {} \n\ttotal alive: {}".format(*list(result))
    )
    await con.close()


def main(
    host, port, max_iters, client_username, client_password, batch_size, overwrite=False, debug=False
):
    """Start event loop to consume stream."""
    if debug:
        LOG.setLevel(logging.DEBUG)
    asyncio.run(
        consumer(host, port, client_username, client_password, max_iters, overwrite, batch_size)
    )


def serve_and_read(filename, port):
    filename = Path(filename)
    if not filename.exists():
        raise FileExistsError(filename)
    dsn = os.getenv("TACVIEW_DATABASE_URL")
    server_proc = Process(target=partial(serve_file.main, filename=filename, port=port))
    server_proc.start()
    try:
        main(
            host="127.0.0.1",
            port=port,
            debug=False,
            max_iters=None,
            batch_size=100000,
            dsn=dsn,
        )
        server_proc.join()
    except Exception as err:
        LOG.error(err)
    finally:
        server_proc.terminate()
