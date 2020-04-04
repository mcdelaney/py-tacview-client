"""Model definitions for database."""
import os
import sys

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from tacview_client.config import DB_URL

engine = sa.create_engine(DB_URL)
metadata = sa.MetaData(engine)
Base = declarative_base(engine, metadata)


class Session(Base): # type: ignore
    __tablename__ = 'session'
    session_id = sa.Column(sa.Integer, primary_key=True)
    start_time = sa.Column(sa.TIMESTAMP(timezone=True), unique=True)
    datasource = sa.Column(sa.String())
    author = sa.Column(sa.String())
    file_version = sa.Column(sa.REAL())
    title = sa.Column(sa.String())
    lat = sa.Column(sa.REAL())
    lon = sa.Column(sa.REAL())
    client_version = sa.Column(sa.String())
    status = sa.Column(sa.String())


class Impact(Base): # type: ignore
    __tablename__ = "impact"
    id = sa.Column(sa.Integer, primary_key=True)
    session_id = sa.Column(sa.INTEGER(), sa.ForeignKey('session.session_id'), index=True)
    killer = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    target = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    weapon = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    time_offset = sa.Column(sa.REAL())
    impact_dist = sa.Column(sa.REAL(), index=True)


class Object(Base): # type: ignore
    __tablename__ = "object"
    id = sa.Column(sa.INTEGER, primary_key=True)
    tac_id = sa.Column(sa.INTEGER)
    session_id = sa.Column(sa.Integer, sa.ForeignKey('session.session_id'), index=True)
    name = sa.Column(sa.String(), index=True)
    color = sa.Column(sa.Enum("Red", "Blue", "Violet", name="color_enum"))
    country = sa.Column(sa.String())
    grp = sa.Column(sa.String())
    pilot = sa.Column(sa.String())
    type = sa.Column(sa.String(), index=True)
    alive = sa.Column(sa.Boolean())
    coalition = sa.Column(sa.Enum("Enemies", "Allies", "Neutral",
                                  name="coalition_enum"))
    first_seen = sa.Column(sa.REAL())
    last_seen = sa.Column(sa.REAL())

    lat = sa.Column(sa.REAL())
    lon = sa.Column(sa.REAL())
    alt = sa.Column(sa.REAL() )
    roll = sa.Column(sa.REAL())
    pitch = sa.Column(sa.REAL())
    yaw = sa.Column(sa.REAL())
    u_coord = sa.Column(sa.REAL())
    v_coord = sa.Column(sa.REAL())
    heading = sa.Column(sa.REAL())
    velocity_kts = sa.Column(sa.REAL())
    impacted = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    impacted_dist = sa.Column(sa.REAL())
    parent = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    parent_dist =sa.Column(sa.REAL())
    updates = sa.Column(sa.Integer())
    can_be_parent = sa.Column(sa.Boolean())


Event = sa.Table(
    "event",
    metadata,
    sa.Column('id', sa.INTEGER(), sa.ForeignKey('object.id'), index=True),
    sa.Column('session_id', sa.INTEGER(), sa.ForeignKey('session.session_id')),
    sa.Column('last_seen', sa.REAL(), index=True),
    sa.Column('alive', sa.Boolean()),
    sa.Column('lat', sa.REAL()),
    sa.Column('lon', sa.REAL()),
    sa.Column('alt', sa.REAL()),
    sa.Column('roll', sa.REAL()),
    sa.Column('pitch', sa.REAL()),
    sa.Column('yaw', sa.REAL()),
    sa.Column('u_coord', sa.REAL()),
    sa.Column('v_coord', sa.REAL()),
    sa.Column('heading', sa.REAL()),
    sa.Column('velocity_kts', sa.REAL()),
    sa.Column('updates', sa.INTEGER()),
    postgresql_partition_by='LIST (session_id)'
)


def connect():
    """Create sa connection to database."""
    try:
        print("Connecting to db....")
        con =  engine.connect()
        print("Connection established...")
        return con
    except Exception as err:
        print(err)
        print("Could not connect to datatbase!"
              " Make sure that the DATABASE_URL environment variable is set!")
        sys.exit(1)


def create_tables():
    """Initalize the database schema."""
    con = connect()
    print("Creating tables...")
    metadata.create_all()
    print("Creating views...")
    con.execute(
        """
        CREATE OR REPLACE VIEW obj_events AS
            SELECT * FROM event evt
            INNER JOIN (SELECT id, session_id, name, color, pilot, first_seen,
                        type, grp, coalition, impacted, parent
                            --,time_offset AS last_offset
                        FROM object) obj
            USING (id, session_id)
        """)

    con.execute(
        """
        CREATE OR REPLACE VIEW parent_summary AS
            SELECT session_id, pilot, name, type, parent, count(*) total,
                count(impacted) as impacts
            FROM (SELECT parent, name, type, impacted, session_id
                  FROM object
                  WHERE parent is not null AND name IS NOT NULL
                  ) objs
            INNER JOIN (
                SELECT id as parent, pilot, session_id
                FROM object where pilot is not NULL
            ) pilots
            USING (parent, session_id)
            GROUP BY session_id, name, type, parent, pilot
        """)

    con.execute(
        sa.text("""
         CREATE OR REPLACE VIEW impact_comb AS (
             SELECT DATE_TRUNC('SECOND',
                (start_time +
                    weapon_first_time*interval '1 second')) kill_timestamp,
                    start_time,
                    killer_name, killer_type, killer as killer_id,
                    target_name, target_type, target as target_id,
                    weapon_name, weapon_type, weapon as weapon_id,
                    id AS impact_id,
                    weapon_first_time, weapon_last_time, session_id,
                    round(cast(impact_dist as numeric), 2) impact_dist,
                    ROUND(cast(weapon_last_time - weapon_first_time as numeric), 2) kill_duration,
                    weapon_color, target_color, killer_color
                FROM  impact
                INNER JOIN (SELECT id killer, pilot killer_name, name killer_type,
                            start_time, color killer_color
                            FROM object
                            INNER JOIN (select session_id, start_time FROM session) sess2
			                USING (session_id)) kill
                USING (killer)
                INNER JOIN (SELECT id target, pilot as target_name, name as target_type,
                            color target_color
                            FROM object) tar
                USING(target)
                INNER JOIN (SELECT id weapon, name AS weapon_name,
                                first_seen as weapon_first_time,
                                last_seen AS weapon_last_time, weapon_type,
                                color weapon_color
                            FROM object
                            LEFT JOIN (SELECT name, category AS weapon_type
                                       FROM weapon_types) weap_type

                            USING (name)
                            WHERE type = 'Weapon+Missile' and NAME IS NOT NULL) weap
                USING (weapon)
                WHERE killer IS NOT NULL AND
                    target IS NOT NULL AND weapon IS NOT NULL
        )
        """))
    con.close()
    print("All tables and views created successfully!")


def drop_tables():
    """Drop all existing tables."""
    con = connect()
    print('Dropping all tables....')
    for table in ['Session', 'Object', 'Event', 'Impact']:
        con.execute(f"drop table if exists {table} CASCADE")
    con.close()
    print("All tables dropped...")
