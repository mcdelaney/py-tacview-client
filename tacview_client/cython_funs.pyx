from libc.math cimport sqrt
from libc.math cimport cos
from libc.math cimport sin
from libc.math cimport M_PI
from libcpp cimport bool


cdef class ObjectRec:
    """Dataclass for objects."""
    cdef public object impacted, parent, Color, Coalition, cart_coords, Type, Name, Country, grp, Pilot
    cdef public int id, tac_id, session_id, updates
    cdef public float first_seen, last_seen, lat, lon, alt, roll, pitch, yaw, u_coord, v_coord, heading, impacted_dist, parent_dist, velocity_kts, secs_since_last_seen
    cdef public bint alive, written, can_be_parent, should_have_parent
    # cdef public list cart_coords

    def __init__(
        self,
        tac_id,
        first_seen,
        last_seen,
        session_id,
    ):
        self.id = 0
        self.tac_id = tac_id
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.session_id = session_id

        self.alive = True
        self.Name = None
        self.Color = None
        self.Country = None
        self.grp = None
        self.Pilot = None
        self.Type = None
        self.Coalition = None
        self.lat = 0.0
        self.lon = 0.0
        self.alt = 1.0  # Ships will have null alt
        self.roll = 0.0
        self.pitch = 0.0
        self.yaw = 0.0
        self.u_coord = 0.0
        self.v_coord = 0.0
        self.heading = 0.0
        self.impacted = None
        self.impacted_dist = -1.0
        self.parent = None
        self.parent_dist = -1.0
        self.updates = 1
        self.velocity_kts = 0.0
        self.can_be_parent = False
        self.should_have_parent = False
        self.secs_since_last_seen = 0
        self.written = False
        self.cart_coords = []


cpdef list get_cartesian_coord(double lat, double lon, double h):
    """Convert coords from geodesic to cartesian."""
    cdef double a = 6378137.0
    cdef double rf = 298.257223563

    cdef double lat_rad = lat * M_PI/180
    cdef double lon_rad = lon * M_PI/180
    cdef double N = sqrt(a / (1 - (1 - (1 - 1 / rf) ** 2) * (sin(lat_rad)) ** 2))
    cdef double X = (N + h) * cos(lat_rad) * cos(lon_rad)
    cdef double Y = (N + h) * cos(lat_rad) * sin(lon_rad)
    cdef double Z = ((1 - 1 / rf) ** 2 * N + h) * sin(lat_rad)
    return [X, Y, Z]


cpdef double compute_dist(list p_1, list p_2):
    """Compute cartesian distance between points."""
    cdef double result = sqrt(
        (p_2[0] - p_1[0]) ** 2 + (p_2[1] - p_1[1]) ** 2 + (p_2[2] - p_1[2]) ** 2
    )
    return result


cpdef ObjectRec compute_velocity(ObjectRec rec):
    """Calculate velocity given the distance from the last point."""
    cdef list new_cart_coords = get_cartesian_coord(rec.lat, rec.lon, rec.alt)
    cdef double velocity_kts
    cdef double t_dist
    if (
        rec.cart_coords
        and rec.secs_since_last_seen
        and rec.secs_since_last_seen > 0.0
    ):
        t_dist = compute_dist(new_cart_coords, rec.cart_coords)
        velocity_kts = (t_dist / rec.secs_since_last_seen) / 1.94384

    rec.cart_coords = new_cart_coords
    if velocity_kts:
        rec.velocity_kts = velocity_kts

    return rec


cpdef ObjectRec proc_line(ObjectRec rec, bytearray raw_line, double ref_lat,
                          double ref_lon, int comma):
    """Parse a textline from tacview into an ObjectRec."""
    cdef tuple COORD_KEYS = (
        "lon",
        "lat",
        "alt",
        "roll",
        "pitch",
        "yaw",
        "u_coord",
        "v_coord",
        "heading",
    )
    cdef int COORD_KEY_LEN = 9

    cdef tuple COORD_KEYS_SHORT = ("lon", "lat", "alt", "u_coord", "v_coord")
    cdef int COORD_KEY_SHORT_LEN = 5

    cdef tuple COORD_KEYS_MED = ("lon", "lat", "alt", "roll", "pitch", "yaw")
    cdef int COORD_KEYS_MED_LEN = 5

    cdef tuple COORD_KEYS_X_SHORT = ("lon", "lat", "alt")
    cdef int COORD_KEYS_X_SHORT_LEN = 3

    cdef bint bytes_remaining = True

    while bytes_remaining:
        last_comma = comma + 1
        comma = raw_line.find(b",", last_comma)
        if comma == -1:
            bytes_remaining = False
            chunk = raw_line[last_comma:]
        else:
            chunk = raw_line[last_comma:comma]
        eq_loc = chunk.find(b"=")
        key = chunk[0:eq_loc]
        val = chunk[eq_loc + 1 :]

        if key == b"T":
            i = 0
            pipe_pos_end = -1
            pipes_remaining = True
            npipe = val.count(b"|")
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
                raise ValueError(
                    "COORD COUNT EITHER 8, 5, OR 4!",
                    npipe,
                    raw_line.decode("UTF-8"),
                )

            while i < C_LEN and pipes_remaining:
                pipe_pos_start = pipe_pos_end + 1
                pipe_pos_end = val.find(b"|", pipe_pos_start)
                if pipe_pos_end == -1:
                    pipes_remaining = False
                    coord = val[pipe_pos_start:]
                else:
                    coord = val[pipe_pos_start:pipe_pos_end]

                if coord != b"":
                    c_key = C_KEYS[i]
                    if c_key == "lat":
                        rec.lat = float(coord) + ref_lat
                    elif c_key == "lon":
                        rec.lon = float(coord) + ref_lon
                    else:
                        setattr(rec, c_key, float(coord))
                i += 1
        else:
            setattr(rec,
                    key.decode("UTF-8") if key != b"Group" else "grp",
                    val.decode("UTF-8"))

    if rec.updates == 1:
        rec.can_be_parent = can_be_parent(rec.Type)
        rec.should_have_parent = should_have_parent(rec.Type)

    rec = compute_velocity(rec)

    return rec


cdef bint can_be_parent(str rec_type):
    """Check if an object is a member of types that could be parents."""
    cdef tuple not_parent_types = (
        "Decoy",
        "Misc",
        "Weapon",
        "Projectile",
        "Ground+Light+Human+Air+Parachutist",
    )

    for t in not_parent_types:
        if t in rec_type:
            return False
    else:
        return True


cdef bint should_have_parent(str rec_type):
    """Check if an object should have a parent record."""
    cdef tuple parented_types = ("Weapon", "Projectile", "Decoy", "Container", "Flare")
    for t in parented_types:
        if t in rec_type:
            return True
    return False
