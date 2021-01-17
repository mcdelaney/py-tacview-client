"""
Tacview client methods.

Results are parsed into usable format, and then written to a postgres database.
"""
import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass
from math import sqrt, cos, sin, radians
from typing import Optional, Any, Dict, Sequence
import time
import pytz
from multiprocessing import Process
from pathlib import Path
from functools import partial
import sys
import os

import asyncpg


try:
    import uvloop  # type: ignore

    uvloop.install()
except (ModuleNotFoundError, NotImplementedError):
    pass

from tacview_client.config import DB_URL, get_logger
from tacview_client.copy_writer import BinCopyWriter
from tacview_client.copy_writer import create_single
from tacview_client.copy_writer import insert_impact
from tacview_client import cython_funs as cyfuns

from tacview_client import serve_file
from tacview_client import __version__


STREAM_PROTOCOL = "XtraLib.Stream.0"
TACVIEW_PROTOCOL = "Tacview.RealTimeTelemetry.0"
HANDSHAKE_TERMINATOR = "\0"


HOST = "147.135.8.169"  # Hoggit Gaw
PORT = 42674
DEBUG = False

LOG = get_logger()


class Ref:
    """Hold and extract Reference values used as offsets."""

    __slots__ = (
        "session_id",
        "lat",
        "lon",
        "title",
        "datasource",
        "author",
        "file_version",
        "start_time",
        "time_offset",
        "all_refs",
        "time_since_last",
        "obj_store",
        "client_version",
        "status",
    )

    def __init__(self):
        self.session_id: Optional[int] = None
        self.lat: Optional[float] = None
        self.lon: Optional[float] = None
        self.title: Optional[str] = None
        self.datasource: Optional[str]
        self.file_version: Optional[float] = None
        self.author: Optional[str]
        self.start_time: Optional[datetime] = None
        self.time_offset: float = 0.0
        self.all_refs: bool = False
        self.time_since_last: float = 0.0
        self.obj_store: Dict[int, cyfuns.ObjectRec] = {}
        self.client_version: str = __version__
        self.status: str = "In Progress"

    def update_time(self, offset):
        """Update the refence time attribute with a new offset."""
        offset = float(offset[1:])
        self.time_since_last = offset - self.time_offset
        self.time_offset = offset

    async def parse_ref_obj(self, line, overwrite=False):
        """
        Attempt to extract ReferenceLatitude, ReferenceLongitude or
        ReferenceTime from a line object.
        """
        try:
            val = line.split(b",")[-1].split(b"=")

            if val[0] == b"ReferenceLatitude":
                LOG.debug("Ref latitude found...")
                self.lat = float(val[1])

            elif val[0] == b"ReferenceLongitude":
                LOG.debug("Ref longitude found...")
                self.lon = float(val[1])

            elif val[0] == b"DataSource":
                LOG.debug("Ref datasource found...")
                self.datasource = val[1].decode("UTF-8")

            elif val[0] == b"Title":
                LOG.debug("Ref Title found...")
                self.title = val[1].decode("UTF-8")

            elif val[0] == b"Author":
                LOG.debug("Ref Author found...")
                self.author = val[1].decode("UTF-8")

            elif val[0] == b"FileVersion":
                LOG.debug("Ref Author found...")
                self.file_version = float(val[1])

            elif val[0] == b"RecordingTime":
                LOG.debug("Ref time found...")
                self.start_time = datetime.strptime(
                    val[1].decode("UTF-8"), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                self.start_time = self.start_time.replace(microsecond=0)
                self.start_time = self.start_time.replace(tzinfo=pytz.UTC)

            self.all_refs = bool(self.lat and self.lon and self.start_time)
            if self.all_refs and not self.session_id:

                if overwrite:
                    async with ASYNC_CON.acquire() as con:
                        await con.execute(
                            f"""DELETE FROM session
                            WHERE start_time = '{self.start_time}'
                            """)

                LOG.info("All Refs found...writing session data to db...")
                sql = """INSERT into session (lat, lon, title,
                                datasource, author, file_version, start_time,
                                client_version, status)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        RETURNING session_id
                """
                async with ASYNC_CON.acquire() as con:
                    self.session_id = await con.fetchval(
                        sql,
                        self.lat,
                        self.lon,
                        self.title,
                        self.datasource,
                        self.author,
                        self.file_version,
                        self.start_time,
                        self.client_version,
                        self.status,
                    )
                LOG.info(f"Creating table partion for {self.session_id}...")
                async with ASYNC_CON.acquire() as con:
                    await con.execute(
                        f"""CREATE TABLE event_{self.session_id} PARTITION OF event
                            FOR VALUES IN ({self.session_id});
                        """
                    )
                LOG.info("Session session data saved...")
        except IndexError:
            pass


async def line_to_obj(raw_line: bytearray, ref: Ref) -> Optional[cyfuns.ObjectRec]:
    """Parse a textline from tacview into an cyfuns.ObjectRec."""
    if raw_line[0:1] == b"-":
        # We know the Object is now dead
        rec = ref.obj_store[int(raw_line[1:], 16)]
        rec.alive = False
        rec.updates += 1

        impacted = cyfuns.determine_contact(rec, ref.obj_store, contact_type=1)
        if impacted:
            rec.impacted = impacted[0]
            rec.impacted_dist = impacted[1]
            await insert_impact(rec, ref.time_offset, ASYNC_CON)
        return rec

    rec = cyfuns.proc_line(raw_line, ref.lat, ref.lon, ref.obj_store,
                           ref.time_offset, ref.session_id)

    return rec

class EndOfFileException(Exception):
    """Throw this exception when the server sends a null string,
    indicating end of file.."""


class MaxIterationsException(Exception):
    """Throw this exception when max iters < total_iters."""


class AsyncStreamReader(Ref):
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
        data = bytearray(await self.reader.readuntil(b"\n"))
        if not data:
            raise EndOfFileException
        return data[:-1]

    async def close(self, status):
        """Close the socket connection and reset ref object."""
        self.writer.close()
        await self.writer.wait_closed()
        LOG.info(f"Marking session status: {status}...")
        await ASYNC_CON.execute(
            """UPDATE session SET status = $1
                                WHERE session_id = $2""",
            status,
            self.session_id,
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
    copy_writer = BinCopyWriter(dsn, batch_size, ASYNC_CON=ASYNC_CON)
    sock = AsyncStreamReader(
        host,
        port,
        client_username,
        client_password,
    )
    await sock.open_connection()
    init_time = time.clock()
    lines_read = 0
    last_log = float(0.0)
    print_log = float(0.0)
    line_proc_time = float(0.0)
    while True:
        try:
            obj = await sock.read_stream()
            LOG.debug(obj)
            lines_read += 1

            if not sock.all_refs:
                await sock.parse_ref_obj(obj, overwrite)
                continue

            if obj[0:1] == b"#":
                sock.update_time(obj)
                if not copy_writer.session_id:
                    copy_writer.session_id = sock.session_id
                await copy_writer.insert_data()

                runtime = time.clock() - init_time
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
            elif obj[0:1] == b"0":
                LOG.debug("Raw line starts with zero...skipping...")
                continue
            else:
                t1 = time.clock()
                obj = await line_to_obj(obj, sock)
                line_proc_time += time.clock() - t1
                if not obj:
                    continue
                if not obj.written:
                    await create_single(obj, ASYNC_CON)
                copy_writer.add_data(obj)

            if max_iters and max_iters < lines_read:
                copy_writer.session_id = sock.session_id
                await copy_writer.insert_data()
                LOG.info(f"Max iters reached: {max_iters}...returning...")
                raise MaxIterationsException

        except (
            MaxIterationsException,
            EndOfFileException,
            asyncio.IncompleteReadError,
        ) as err:
            LOG.info(f"Starting shutdown due to: {err.__class__.__name__}")
            await copy_writer.cleanup()
            await sock.close(status="Success")

            total_time = time.clock() - init_time
            LOG.info("Total Lines Processed : %s", str(lines_read))
            LOG.info("Total seconds running : %.2f", total_time)
            LOG.info(f"Total db write time: {copy_writer.db_event_time}")
            LOG.info(
                "Pct Event Write Time: %.2f", copy_writer.db_event_time / total_time
            )

            LOG.info("Pct Line Proc Time: %.2f", line_proc_time / total_time)
            LOG.info("Total Line Proc Secs: %.2f", line_proc_time)
            LOG.info("Lines Proc/Sec: %.2f", lines_read / line_proc_time)
            LOG.info("Total Lines/second: %.4f", lines_read / total_time)
            total = {}
            for obj in sock.obj_store.values():
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
            await sock.close(status="Error")

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
        "Results:\nobjects: {} \nparents: {} \nimpacts: {}"
        "\nmax_updates: {} \ntotal updates: {}"
        "\ntotal events: {} \ntotal alive: {}".format(*list(result))
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
