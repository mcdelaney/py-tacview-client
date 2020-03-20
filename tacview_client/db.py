"""Model definitions for database."""
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from tacview_client.config import DB_URL

engine = sa.create_engine(DB_URL)
metadata = sa.MetaData(engine)
Base = declarative_base(engine, metadata)


class Session(Base): # type: ignore
    __tablename__ = 'session'
    session_id = sa.Column(sa.Integer, primary_key=True)
    start_time = sa.Column(sa.TIMESTAMP(), unique=True)
    datasource = sa.Column(sa.String())
    author = sa.Column(sa.String())
    file_version = sa.Column(sa.Float())
    title = sa.Column(sa.String())
    lat = sa.Column(sa.Float())
    lon = sa.Column(sa.Float())


class Impact(Base): # type: ignore
    __tablename__ = "impact"
    id = sa.Column(sa.Integer, primary_key=True)
    session_id = sa.Column(sa.INTEGER(), sa.ForeignKey('session.session_id'))
    killer = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    target = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    weapon = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    time_offset = sa.Column(sa.Numeric())
    impact_dist = sa.Column(sa.Numeric())


class Object(Base): # type: ignore
    __tablename__ = "object"
    id = sa.Column(sa.INTEGER, primary_key=True)
    tac_id = sa.Column(sa.INTEGER)
    session_id = sa.Column(sa.Integer, sa.ForeignKey('session.session_id'))
    name = sa.Column(sa.String())
    color = sa.Column(sa.Enum("Red", "Blue", "Violet", name="color_enum"))
    country = sa.Column(sa.String())
    grp = sa.Column(sa.String())
    pilot = sa.Column(sa.String())
    type = sa.Column(sa.String())
    alive = sa.Column(sa.Boolean())
    coalition = sa.Column(sa.Enum("Enemies", "Allies", "Neutral",
                                  name="coalition_enum"))
    first_seen = sa.Column(sa.Float())
    last_seen = sa.Column(sa.Float())

    lat = sa.Column(sa.Float())
    lon = sa.Column(sa.Float())
    alt = sa.Column(sa.Float() )
    roll = sa.Column(sa.Float())
    pitch = sa.Column(sa.Float())
    yaw = sa.Column(sa.Float())
    u_coord = sa.Column(sa.Float())
    v_coord = sa.Column(sa.Float())
    heading = sa.Column(sa.Float())
    velocity_kts = sa.Column(sa.Float())
    impacted = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    impacted_dist = sa.Column(sa.Float())
    parent = sa.Column(sa.INTEGER(), sa.ForeignKey('object.id'))
    parent_dist =sa.Column(sa.Float())
    updates = sa.Column(sa.Integer())


Event = sa.Table(
    "event",
    metadata,
    sa.Column('id', sa.INTEGER(), sa.ForeignKey('object.id')),
    sa.Column('session_id', sa.INTEGER(), sa.ForeignKey('session.session_id')),
    sa.Column('last_seen', sa.Float()),
    sa.Column('alive', sa.Boolean()),
    sa.Column('lat', sa.Float()),
    sa.Column('lon', sa.Float()),
    sa.Column('alt', sa.Float()),
    sa.Column('roll', sa.Float()),
    sa.Column('pitch', sa.Float()),
    sa.Column('yaw', sa.Float()),
    sa.Column('u_coord', sa.Float()),
    sa.Column('v_coord', sa.Float()),
    sa.Column('heading', sa.Float()),
    sa.Column('velocity_kts', sa.Float()),
    sa.Column('updates', sa.INTEGER())
)


def drop_and_recreate_tables():
    """Initialize the database and execute create table statements."""
    con = engine.connect()

    for table in ['Session', 'Object', 'Event', 'Impact']:
        con.execute(f"drop table if exists {table} CASCADE")

    metadata.create_all()

    con.execute(
        """
        CREATE VIEW obj_events AS
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

    con.close()
