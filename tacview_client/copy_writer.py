"""
Classes managing binary copy protocol to postgres.
"""

from datetime import datetime
from io import BytesIO
from uuid import uuid1
import struct
from typing import List

import asyncpg

from tacview_client.config import get_logger


LOG = get_logger()


class BinCopyWriter:
    """Manage efficient insertion of bulk data to postgres."""

    db_event_time: float = 0.0
    event_times: List = []
    insert: BytesIO
    fmt_str: str = ">h ii ii if i? if if if if if if if if if if ii"
    copy_header = struct.pack(">11sii", b"PGCOPY\n\377\r\n\0", 0, 0)
    copy_trailer = struct.pack(">h", -1)


    def __init__(self, dsn: str, batch_size: int = 10000, session_id=None, ref=None):
        self.dsn = dsn
        self.batch_size = batch_size
        self.create_byte_buffer()
        self.insert_count = 0
        self.session_id = session_id
        self.tbl_uuid = str(uuid1()).replace("-", "_")
        self.impacts = []
        self.ref = ref
        self.con = None
        self.flush_stmt = None
        self.single_insert_stmt = None

    async def setup(self):
        self.con = await asyncpg.connect(self.dsn)
        sql = """
            INSERT into impact
                (session_id, killer, target, weapon, time_offset, impact_dist)
                VALUES($1, $2, $3, $4, $5, $6)
        """
        self.flush_stmt = await self.con.prepare(sql)
        self.single_insert_stmt = await self.prep_single_insert_stmt()

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

    def add_impact(self, impact):
        self.impacts.append(
            (
                impact.session_id,
                impact.parent,
                impact.impacted,
                impact.id,
                self.ref.time_offset,
                impact.impacted_dist,
            )
        )

    def create_byte_buffer(self) -> None:
        self.insert = BytesIO()
        self.insert.write(self.copy_header)
        self.insert_count = 0

    def add_data(self, obj) -> None:
        """Take an ObjectRec, pack it to bytes, then write to byte buffer."""
        try:
            data = (
                15,
                4,
                obj.id,
                4,
                obj.session_id,
                4,
                obj.last_seen,
                1,
                obj.alive,
                4,
                obj.lat,
                4,
                obj.lon,
                4,
                obj.alt,
                4,
                obj.roll,
                4,
                obj.pitch,
                4,
                obj.yaw,
                4,
                obj.u_coord,
                4,
                obj.v_coord,
                4,
                obj.heading,
                4,
                obj.velocity_kts,
                4,
                obj.updates,
            )
            packed = struct.pack(self.fmt_str, *data)
        except Exception as err:
            LOG.error([obj.tac_id, obj.last_seen])
            raise err

        self.insert.write(packed)
        self.insert_count += 1

    async def cleanup(self) -> None:
        """Shut down and ensure all data is written."""
        LOG.info("Shutting down copywriter....")
        self.min_insert_size = -1  # ensure everything gets flushed
        await self.insert_data_maybe(force=True)
        self.db_event_time = sum(self.event_times)

    async def insert_data_maybe(self, force: bool = False):
        if self.batch_size > self.insert_count and not force:
            LOG.debug("Not enough data for insert....")
            return

        t1 = datetime.now()
        LOG.info(f"Inserting {self.insert_count} events and flushing {len(self.impacts)} impacts...")
        self.make_query()
        self.insert.write(self.copy_trailer)
        self.insert.seek(0)

        copy_future = self.con._copy_in(self.cmd, self.insert, 100)
        if len(self.impacts) > 0:
            for impact in self.impacts:
                await self.flush_stmt.fetchval(*impact)
            # impact_future = self.flush_impacts(con)
        await copy_future
        self.insert.close()

        self.create_byte_buffer()
        self.impacts = []
        self.event_times.append((datetime.now() - t1).total_seconds())

    async def prep_single_insert_stmt(self):
        """Create the prepared statement for single record inserts."""
        sql = """INSERT into object (
                tac_id, session_id, name, color, country, grp, pilot, type,
                alive, coalition, first_seen, last_seen, lat, lon, alt, roll,
                pitch, yaw, u_coord, v_coord, heading, velocity_kts, impacted,
                impacted_dist, parent, parent_dist, updates, can_be_parent
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
            $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)

            RETURNING id
            """
        stmt = await self.con.prepare(sql)
        return stmt

    async def create_single(self, obj):
        """Insert a single newly create record to database."""
        vals = (
            obj.tac_id,
            obj.session_id,
            obj.Name,
            obj.Color,
            obj.Country,
            obj.grp,
            obj.Pilot,
            obj.Type,
            obj.alive,
            obj.Coalition,
            obj.first_seen,
            obj.last_seen,
            obj.lat,
            obj.lon,
            obj.alt,
            obj.roll,
            obj.pitch,
            obj.yaw,
            obj.u_coord,
            obj.v_coord,
            obj.heading,
            obj.velocity_kts,
            obj.impacted,
            obj.impacted_dist,
            obj.parent,
            obj.parent_dist,
            obj.updates,
            obj.can_be_parent,
        )

        obj.id = await self.single_insert_stmt.fetchval(*vals)
        obj.written = True
