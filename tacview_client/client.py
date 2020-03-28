"""
Tacview client methods.

Results are parsed into usable format, and then written to a postgres database.
"""
from io import BytesIO
import asyncio
import logging
from datetime import datetime
from math import sqrt, cos, sin, radians
from typing import Optional, Any, Dict, List, Sequence
import time
import struct
import pytz
from multiprocessing import Process
from pathlib import Path
from functools import partial
from uuid import uuid1
import sys
import os

import asyncpg
import uvloop

from tacview_client.config import DB_URL, get_logger
from tacview_client import serve_file
from tacview_client import __version__

CLIENT = 'tacview-client'
PASSWORD = '0'
STREAM_PROTOCOL = "XtraLib.Stream.0"
TACVIEW_PROTOCOL = 'Tacview.RealTimeTelemetry.0'
HANDSHAKE_TERMINATOR = "\0"

HANDSHAKE = ('\n'.join([STREAM_PROTOCOL, TACVIEW_PROTOCOL, CLIENT, PASSWORD]) +
             HANDSHAKE_TERMINATOR).encode('utf-8')

COORD_KEYS = ('lon', 'lat', 'alt', 'roll', 'pitch', 'yaw', 'u_coord',
              'v_coord', 'heading')
COORD_KEY_LEN = len(COORD_KEYS)

COORD_KEYS_SHORT = ('lon', 'lat', 'alt', 'u_coord', 'v_coord')
COORD_KEY_SHORT_LEN = len(COORD_KEYS_SHORT)

COORD_KEYS_MED = ('lon', 'lat', 'alt', 'roll', 'pitch', 'yaw')
COORD_KEYS_MED_LEN = len(COORD_KEYS_MED)

COORD_KEYS_X_SHORT = ('lon', 'lat', 'alt')
COORD_KEYS_X_SHORT_LEN = len(COORD_KEYS_X_SHORT)

HOST = '147.135.8.169'  # Hoggit Gaw
PORT = 42674
DEBUG = False
CONTACT_TIME = 0.0

LOG = get_logger()


class Ref:  # pylint: disable=too-many-instance-attributes
    """Hold and extract Reference values used as offsets."""
    __slots__ = ('session_id', 'lat', 'lon', 'title', 'datasource', 'author',
                 'file_version', 'start_time', 'time_offset', 'all_refs',
                 'time_since_last', 'obj_store', 'client_version',
                 'status')
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
        self.obj_store: Dict[int, ObjectRec] = {}
        self.client_version: str = __version__
        self.status: str = 'In Progress'

    def update_time(self, offset):
        """Update the refence time attribute with a new offset."""
        offset = float(offset[1:])
        self.time_since_last = offset - self.time_offset
        self.time_offset = offset

    async def parse_ref_obj(self, line):
        """
        Attempt to extract ReferenceLatitude, ReferenceLongitude or
        ReferenceTime from a line object.
        """
        try:
            val = line.split(b',')[-1].split(b'=')

            if val[0] == b'ReferenceLatitude':
                LOG.debug('Ref latitude found...')
                self.lat = float(val[1])

            elif val[0] == b'ReferenceLongitude':
                LOG.debug('Ref longitude found...')
                self.lon = float(val[1])

            elif val[0] == b'DataSource':
                LOG.debug('Ref datasource found...')
                self.datasource = val[1].decode('UTF-8')

            elif val[0] == b'Title':
                LOG.debug('Ref Title found...')
                self.title = val[1].decode('UTF-8')

            elif val[0] == b'Author':
                LOG.debug('Ref Author found...')
                self.author = val[1].decode('UTF-8')

            elif val[0] == b'FileVersion':
                LOG.debug('Ref Author found...')
                self.file_version = float(val[1])

            elif val[0] == b'RecordingTime':
                LOG.debug('Ref time found...')
                self.start_time = datetime.strptime(val[1].decode('UTF-8'),
                                              '%Y-%m-%dT%H:%M:%S.%fZ')
                self.start_time = self.start_time.replace(microsecond=0)
                self.start_time = self.start_time.replace(tzinfo=pytz.UTC)

            self.all_refs = bool(self.lat and self.lon and self.start_time)
            if self.all_refs and not self.session_id:
                LOG.info("All Refs found...writing session data to db...")
                sql = """INSERT into session (lat, lon, title,
                                datasource, author, file_version, start_time,
                                client_version, status)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        RETURNING session_id
                """
                async with ASYNC_CON.acquire() as con:
                    self.session_id = await con.fetchval(
                        sql, self.lat, self.lon, self.title, self.datasource,
                        self.author, self.file_version, self.start_time,
                        self.client_version, self.status)
                LOG.info(f"Creating table partion for {self.session_id}...")
                async with ASYNC_CON.acquire() as con:
                    await con.execute(
                        f"""CREATE TABLE event_{self.session_id} PARTITION OF event
                            FOR VALUES IN ({self.session_id});
                        """)
                LOG.info("Session session data saved...")
        except IndexError:
            pass

from dataclasses import dataclass
@dataclass
class ObjectRec:
    __slots__ = (
        'id', 'tac_id', 'first_seen', 'last_seen', 'session_id', 'alive', 'Name',
        'Color', 'Country', 'grp', 'Pilot', 'Type', 'Coalition', 'lat', 'lon',
        'alt', 'roll', 'pitch', 'yaw', 'u_coord', 'v_coord', 'heading',
        'impacted', 'impacted_dist', 'parent', 'parent_dist', 'updates',
        'velocity_kts', 'secs_since_last_seen', 'written', 'cart_coords'
    )

    def __init__(
            self,
            tac_id: int = None,
            first_seen: Optional[float] = None,
            last_seen: Optional[float] = None,
            session_id: Optional[int] = None):
        self.id = None
        self.tac_id = tac_id
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.session_id = session_id

        self.alive: bool = True
        self.Name: Optional[str] = None
        self.Color: Optional[str] = None
        self.Country: Optional[str] = None
        self.grp: Optional[str] = None
        self.Pilot: Optional[str] = None
        self.Type: Optional[str] = None
        self.Coalition: Optional[str] = None
        self.lat: Optional[float] = None
        self.lon: Optional[float] = None
        self.alt: Optional[float] = 1  # Ships will have null alt
        self.roll: Optional[float] = 0.0
        self.pitch: Optional[float] = 0.0
        self.yaw: Optional[float] = 0.0
        self.u_coord: Optional[float] = 0.0
        self.v_coord: Optional[float] = 0.0
        self.heading: Optional[float] = 0.0
        self.impacted: Optional[int] = None
        self.impacted_dist: Optional[float] = None
        self.parent: Optional[int] = None
        self.parent_dist: Optional[float] = None
        self.updates: int = 1
        self.velocity_kts: float = 0.0
        self.secs_since_last_seen: Optional[float] = None
        self.written: bool = False
        self.cart_coords: Optional[Sequence] = None

    def update_val(self, key: str, value: Any) -> None:
        """Set value for a given key."""
        setattr(self, key, value)

    def update_last_seen(self, value: float) -> None:
        """Set the last seen time offset for a record."""
        self.secs_since_last_seen = value - self.last_seen
        self.last_seen = value

    def compute_velocity(self) -> None:
        """Calculate velocity given the distance from the last point."""
        new_cart_coords = get_cartesian_coord(self.lat, self.lon, self.alt)
        if self.cart_coords and self.secs_since_last_seen and self.secs_since_last_seen > 0:
            t_dist = compute_dist(new_cart_coords, self.cart_coords)
            self.velocity_kts = (t_dist / self.secs_since_last_seen) / 1.94384
        self.cart_coords = new_cart_coords

    def should_have_parent(self) -> bool:
        """Check if an object should have a parent record."""
        parented_types = ('weapon', 'projectile', 'decoy', 'container', 'flare')
        tval = self.Type.lower()
        for t in parented_types:
            if t in tval:
                return True
        return False

    def can_be_parent(self) -> bool:
        """Check if an object is a member of types that could be parents."""
        not_parent_types = ('Decoy', 'Misc', 'Weapon', 'Projectile',
                            'Ground+Light+Human+Air+Parachutist')
        tval = self.Type
        for t in not_parent_types:
            if t in tval:
                return False
        else:
            return True


def get_cartesian_coord(lat, lon, h) -> Sequence:
    """Convert coords from geodesic to cartesian."""
    a = 6378137.0
    rf = 298.257223563
    lat_rad = radians(lat)
    lon_rad = radians(lon)
    N = sqrt(a / (1 - (1 - (1 - 1 / rf) ** 2) * (sin(lat_rad)) ** 2))
    X = (N + h) * cos(lat_rad) * cos(lon_rad)
    Y = (N + h) * cos(lat_rad) * sin(lon_rad)
    Z = ((1 - 1 / rf) ** 2 * N + h) * sin(lat_rad)
    return X, Y, Z


def compute_dist(p_1: Sequence, p_2: Sequence) -> float:
    """Compute cartesian distance between points."""
    return sqrt((p_2[0] - p_1[0])**2 + (p_2[1] - p_1[1])**2 +
                (p_2[2] - p_1[2])**2)


async def determine_contact(rec, ref: Ref, contact_type='parent'):
    """Determine the parent of missiles, rockets, and bombs."""
    global CONTACT_TIME
    t1 = time.time()

    LOG.debug(f"Determing {contact_type} for object id: %s -- %s-%s...", rec.id,
              rec.Name, rec.Type)

    if contact_type == "parent":
        if rec.Color == 'Violet':
            acpt_colors = ['Red', 'Blue']
        else:
            acpt_colors = [rec.Color]

    elif contact_type == 'impacted':
        acpt_colors = ['Red'] if rec.Color == 'Blue' else ['Blue']

    else:
        raise NotImplementedError

    closest = []
    n_checked = 0
    offset_time = rec.last_seen - 2.5

    for near in ref.obj_store.values():
        if not near.can_be_parent or near.tac_id == rec.tac_id or \
            near.Color not in acpt_colors:
            continue
        if contact_type == 'impacted' and not near.Type.startswith('Air+'):
            continue

        if (offset_time > near.last_seen and (
            not ('Ground' in near.Type.lower() and near.alive == True))):
            continue

        n_checked += 1
        prox = compute_dist(rec.cart_coords, near.cart_coords) # type: ignore
        LOG.debug("Distance to rec %s-%s is %d...", near.Name, near.Type, prox)
        if not closest or (prox < closest[1]):
            closest = [near.id, prox, near.Name, near.Pilot, near.Type]
    CONTACT_TIME += (time.time() - t1)

    if not closest:
        return None

    if closest[1] > 200 and contact_type == 'parent':
        LOG.warning(
            f"Rejecting closest {contact_type} for "
            f"{rec.id}-{rec.Name}-{rec.Type}: "
            f"{closest[4]} {closest[1]}m...{n_checked} checked!")
        return None

    return closest


async def line_to_obj(raw_line: bytearray, ref: Ref) -> Optional[ObjectRec]:
    """Parse a textline from tacview into an ObjectRec."""
    # secondary_update = None
    if raw_line[0:1] == b"0":
        LOG.debug("Raw line starts with 0...")
        return None

    if raw_line[0:1] == b'-':
        rec = ref.obj_store[int(raw_line[1:], 16)]
        rec.alive = False
        rec.updates += 1

        if 'Weapon' in rec.Type or 'Projectile' in rec.Type:
            impacted = await determine_contact(rec, contact_type='impacted',
                                               ref=ref)
            if impacted:
                rec.impacted = impacted[0]
                rec.impacted_dist = impacted[1]
                await insert_impact(rec, ref.time_offset)
        return rec

    comma = raw_line.find(b',')
    rec_id = int(raw_line[0:comma], 16)
    try:
        rec = ref.obj_store[rec_id]
        rec.update_last_seen(ref.time_offset)
        rec.updates += 1

    except KeyError:
        # Object not yet seen...create new record...
        rec = ObjectRec(tac_id=rec_id,
                        session_id=ref.session_id,
                        first_seen=ref.time_offset,
                        last_seen=ref.time_offset)
        ref.obj_store[rec_id] = rec

    bytes_remaining = True
    try:
        while bytes_remaining:
            last_comma = comma + 1
            comma = raw_line.find(b',', last_comma)
            if comma == -1:
                bytes_remaining = False
                chunk = raw_line[last_comma:]
            else:
                chunk = raw_line[last_comma:comma]
            eq_loc = chunk.find(b"=")
            key = chunk[0:eq_loc]
            val = chunk[eq_loc + 1:]

            if key == b"T":
                i = 0
                pipe_pos_end = -1
                pipes_remaining = True
                npipe = val.count(b'|')
                if npipe == 8:
                    C_KEYS = COORD_KEYS
                    C_LEN = COORD_KEY_LEN
                elif npipe == 5:
                    C_KEYS = COORD_KEYS_MED
                    C_LEN = COORD_KEYS_MED_LEN
                elif npipe == 4:
                    C_KEYS = COORD_KEYS_SHORT
                    C_LEN = COORD_KEY_SHORT_LEN
                elif npipe == 2:
                    C_KEYS = COORD_KEYS_X_SHORT
                    C_LEN = COORD_KEYS_X_SHORT_LEN
                else:
                    LOG.error("Coord count error!")
                    raise ValueError("COORD COUNT EITHER 8, 5, OR 4!", npipe, raw_line.decode('UTF-8'))

                while i < C_LEN and pipes_remaining:
                    pipe_pos_start = pipe_pos_end + 1
                    pipe_pos_end = val.find(b'|', pipe_pos_start)
                    if pipe_pos_end == -1:
                        pipes_remaining = False
                        coord = val[pipe_pos_start:]
                    else:
                        coord = val[pipe_pos_start:pipe_pos_end]

                    if coord != b'':
                        c_key = C_KEYS[i]
                        if c_key == "lat":
                            rec.lat = float(coord) + ref.lat
                        elif c_key == "lon":
                            rec.lon = float(coord) + ref.lon
                        else:
                            rec.update_val(c_key, float(coord))
                    i += 1
            else:
                rec.update_val(
                    key.decode('UTF-8') if key != b'Group' else 'grp', val.decode('UTF-8'))
    except Exception as err:
        raise err

    rec.compute_velocity()

    if rec.updates == 1 and rec.should_have_parent():
        parent_info = await determine_contact(rec, contact_type='parent',
                                              ref=ref)
        if parent_info:
            rec.parent = parent_info[0]
            rec.parent_dist = parent_info[1]

    return rec


async def insert_impact(rec, impact_time):
    sql = """INSERT into impact (session_id, killer, target,
                    weapon, time_offset, impact_dist)
                VALUES($1, $2, $3, $4, $5, $6)
    """
    vals = (rec.session_id, rec.parent, rec.impacted, rec.id,
            impact_time, rec.impacted_dist)
    async with ASYNC_CON.acquire() as con:
        await con.execute(sql, *vals)


async def create_single(obj):
    """Insert a single newly create record to database."""
    vals = (obj.tac_id,
            obj.session_id,
            obj.Name,
            obj.Color,
            obj.Country,
            obj.grp,
            obj.Pilot,
            obj.Type,
            obj.alive,
            obj.Coalition,
            obj.first_seen, obj.last_seen, obj.lat, obj.lon, obj.alt, obj.roll,
            obj.pitch, obj.yaw, obj.u_coord, obj.v_coord, obj.heading,
            obj.velocity_kts, obj.impacted, obj.impacted_dist,
            obj.parent, obj.parent_dist, obj.updates,
            obj.can_be_parent())

    sql =  """INSERT into object (
            tac_id, session_id, name, color, country, grp, pilot, type,
            alive, coalition, first_seen, last_seen, lat, lon, alt, roll,
            pitch, yaw, u_coord, v_coord, heading, velocity_kts, impacted,
            impacted_dist, parent, parent_dist, updates, can_be_parent
        )
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
           $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)

        RETURNING id
        """
    async with ASYNC_CON.acquire() as con:
        obj.id = await con.fetchval(sql, *vals)
    obj.written = True


class EndOfFileException(Exception):
    """Throw this exception when the server sends a null string,
    indicating end of file.."""


class MaxIterationsException(Exception):
    """Throw this exception when max iters < total_iters."""


class AsyncStreamReader(Ref):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    """Read from Tacview socket."""
    def __init__(self, host, port, debug=False):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.sink = "log/raw_sink.txt"
        self.debug = debug
        if self.debug:
            open(self.sink, 'w').close()

    async def open_connection(self):
        """
        Initialize the socket connection and write handshake data.
        If connection fails, wait 3 seconds and retry.
        """
        while True:
            try:
                LOG.info(f'Opening connection to {self.host}:{self.port}...')
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port)
                LOG.info('Connection opened...sending handshake...')
                self.writer.write(HANDSHAKE)
                await self.reader.readline()
                LOG.info('Connection opened with successful handshake...')
                break
            except ConnectionError:
                LOG.error('Connection attempt failed....retrying in 3 sec...')
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
        await ASYNC_CON.execute("""UPDATE session SET status = $1
                                WHERE session_id = $2""",
                                status, self.session_id)
        LOG.info(f'Session marked as {status}!')


class BinCopyWriter:
    """Manage efficient insertion of bulk data to postgres."""
    db_event_time: float = 0.0
    event_times: List = []
    insert: BytesIO
    fmt_str: str = '>h ii ii if i? if if if if if if if if if if ii'
    copy_header = struct.pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0)
    copy_trailer =  struct.pack('>h', -1)


    # """
    # WITH src AS (
    #     UPDATE serial_rate
    #     SET rate = 22.53, serial_key = '0002'
    #     WHERE serial_key = '002' AND id = '01'
    #     RETURNING *
    #     )
    # UPDATE serial_table dst
    # SET serial_key = src.serial_key
    # FROM src
    # -- WHERE dst.id = src.id AND dst.serial_key  = '002'
    # WHERE dst.id = '01' AND dst.serial_key  = '002';
    # """

    def __init__(self, dsn: str, batch_size: int = 10000, session_id=None):
        self.dsn = dsn
        self.batch_size = batch_size
        self.build_fmt_string()
        self.create_byte_buffer()
        self.insert_count = 0
        self.session_id = session_id
        self.tbl_uuid = str(uuid1()).replace("-", '_')

    def make_query(self):
        self.cmd = f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS "{self.tbl_uuid}"
                (LIKE event_{self.session_id} INCLUDING DEFAULTS);

            COPY public."{self.tbl_uuid}" FROM STDIN WITH BINARY;

            CREATE INDEX tmp_idx on "{self.tbl_uuid}" (id, updates DESC);

            INSERT INTO event_{self.session_id}
            SELECT * FROM "{self.tbl_uuid}";

            INSERT INTO object (
                id, session_id, last_seen, alive, lat, lon, alt, roll, pitch,
                yaw, u_coord, v_coord, heading, velocity_kts, updates
            )
            SELECT id, session_id, last_seen, alive, lat, lon, alt, roll,
                pitch, yaw, u_coord, v_coord, heading, velocity_kts, updates
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY id
                        ORDER BY updates DESC) as row_number
                FROM "{self.tbl_uuid}"
            ) evt
            WHERE row_number = 1
            ON CONFLICT (id)
            DO UPDATE SET session_id=EXCLUDED.session_id,
                last_seen=EXCLUDED.last_seen,
                alive=EXCLUDED.alive,
                lat=EXCLUDED.lat,
                lon=EXCLUDED.lon,
                alt=EXCLUDED.alt,
                roll=EXCLUDED.roll,
                pitch=EXCLUDED.pitch,
                yaw=EXCLUDED.yaw,
                u_coord=EXCLUDED.u_coord,
                v_coord=EXCLUDED.v_coord,
                heading=EXCLUDED.heading,
                velocity_kts=EXCLUDED.velocity_kts,
                updates=EXCLUDED.updates
            WHERE object.updates < EXCLUDED.updates;

            DROP INDEX tmp_idx;
            DROP TABLE "{self.tbl_uuid}";
        """

    def build_fmt_string(self):
        {
            "INTEGER": ('ii', 4),
            "FLOAT": ('id', 8),
            "DOUBLE": ('id', 8),
            "NUMERIC": ('id', 8)
        }

    def create_byte_buffer(self) -> None:
        self.insert = BytesIO()
        self.insert.write(self.copy_header)
        self.insert_count = 0

    def add_data(self, obj: ObjectRec) -> None:
        """Take an ObjectRec, pack it to bytes, then write to byte buffer."""
        try:
            data = (15,
                    4, obj.id,
                    4, obj.session_id,
                    4, obj.last_seen,
                    1, obj.alive,
                    4, obj.lat,
                    4, obj.lon,
                    4, obj.alt,
                    4, obj.roll,
                    4, obj.pitch,
                    4, obj.yaw,
                    4, obj.u_coord,
                    4, obj.v_coord,
                    4, obj.heading,
                    4, obj.velocity_kts,
                    4, obj.updates)

            packed = struct.pack(self.fmt_str, *data)
        except Exception as err:
            LOG.error([obj.tac_id, obj.last_seen])
            raise err

        self.insert.write(packed)
        self.insert_count += 1

    async def cleanup(self) -> None:
        """Shut down and ensure all data is written."""
        self.min_insert_size = -1 # ensure everything gets flushed
        await self.insert_data(force=True)
        self.db_event_time = sum(self.event_times)

    async def insert_data(self, force=False) -> None:
        if not force and self.batch_size > self.insert_count:
            LOG.debug("Not enough data for insert....")
            return
        self.make_query()
        LOG.debug(f'Inserting {self.insert_count} records...')
        self.insert.write(self.copy_trailer)
        self.insert.seek(0)
        async with ASYNC_CON.acquire() as con:
            await con._copy_in(self.cmd , self.insert, 100)
        self.insert.close()
        self.create_byte_buffer()


async def consumer(host: str,
                   port: int,
                   max_iters: Optional[int],
                   batch_size: int,
                   dsn:str) -> None:
    """Main method to consume stream."""
    LOG.info("Starting tacview client with settings: "
             "debug: %s -- batch-size: %s", DEBUG, batch_size)
    global ASYNC_CON
    ASYNC_CON = await asyncpg.create_pool(DB_URL)
    copy_writer = BinCopyWriter(dsn, batch_size)
    sock = AsyncStreamReader(host, port)
    await sock.open_connection()
    init_time = time.time()
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
                await sock.parse_ref_obj(obj)
                continue

            if obj[0:1] == b"#":
                sock.update_time(obj)
                if not copy_writer.session_id:
                    copy_writer.session_id = sock.session_id
                await copy_writer.insert_data()

                runtime = time.time() - init_time
                log_check = runtime - last_log
                print_check = runtime - print_log
                if log_check > 0.05:
                    ln_sec = lines_read / runtime
                    sys.stdout.write("\rEvents processed: {:,} at {:,.2f} events/sec".format(
                        lines_read, ln_sec))
                    sys.stdout.flush()
                    last_log = runtime

                    if print_check > 10:
                        LOG.info("Events processed: {:,} at {:,.2f} events/sec".format(
                            lines_read, ln_sec))
                        print_log = runtime

            else:
                t1 = time.time()
                obj = await line_to_obj(obj, sock)
                line_proc_time += (time.time() - t1)
                if not obj:
                    continue
                if not obj.written:
                    await create_single(obj)
                copy_writer.add_data(obj)

            if max_iters and max_iters < lines_read:
                copy_writer.session_id = sock.session_id
                await copy_writer.insert_data()
                LOG.info(f"Max iters reached: {max_iters}...returning...")
                raise MaxIterationsException

        except (MaxIterationsException, EndOfFileException,
                asyncio.IncompleteReadError) as err:
            LOG.info(f'Starting shutdown due to: {err.__class__.__name__}')
            await copy_writer.cleanup()
            await sock.close(status='Success')

            total_time = time.time() - init_time
            LOG.info('Total Lines Processed : %s', str(lines_read))
            LOG.info('Total seconds running : %.2f', total_time)
            LOG.info('Pct Event Write Time: %.2f',
                     copy_writer.db_event_time / total_time)
            LOG.info('Pct Get Contact Time: %.2f', CONTACT_TIME / total_time)
            LOG.info('Pct Line Proc Time: %.2f', line_proc_time / total_time)
            LOG.info('Lines/second: %.4f', lines_read / total_time)
            total = {}
            for obj in sock.obj_store.values():
                if obj.should_have_parent() and not obj.parent:
                    try:
                        total[obj.Type] += 1
                    except KeyError:
                        total[obj.Type] = 1
            for key, value in total.items():
                LOG.info(f"total without parent but should {key}: {value}")

            LOG.info('Exiting tacview-client!')
            return

        except asyncpg.UniqueViolationError as err:
            LOG.error("The file you are trying to process is already in the database! "
                        "To re-process it, delete all associated rows.")
            await sock.close(status='Error')
            raise err

        except Exception as err:
            await sock.close(status='Error')

            LOG.error("Unhandled Exception!"
                      "Writing remaining updates to db and exiting!")
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
                FROM object""")

    print("Results:\nobjects: {} \nparents: {} \nimpacts: {}"
            "\nmax_updates: {} \ntotal updates: {}"
            "\ntotal events: {} \ntotal alive: {}".format(*list(result)))
    await con.close()

def main(host, port, max_iters, batch_size, dsn, debug=False):
    """Start event loop to consume stream."""
    uvloop.install()
    if debug:
        LOG.setLevel(logging.DEBUG)
    asyncio.run(consumer(host, port, max_iters, batch_size, dsn))


def serve_and_read(filename, port):
    filename = Path(filename)
    if not filename.exists():
        raise FileExistsError(filename)
    dsn = os.getenv("DATABASE_URL")
    server_proc = Process(target=partial(
        serve_file.main, filename=filename, port=port))
    server_proc.start()
    try:
        main(host='127.0.0.1',
                    port=port,
                    debug=False,
                    max_iters=None,
                    batch_size=100000,
                    dsn=dsn)
        server_proc.join()
    except Exception as err:
        LOG.error(err)
    finally:
        server_proc.terminate()
