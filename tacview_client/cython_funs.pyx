from libc.math cimport sqrt
from libc.math cimport cos
from libc.math cimport sin
from libc.math cimport M_PI
from libcpp cimport bool

import numpy as np
cimport numpy as np
np.import_array()
# ctypedef np.int_t DTYPE_t

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

cdef tuple NON_PARENTED_TYPES = (
    "Decoy",
    "Misc",
    "Weapon",
    "Projectile",
    "Ground+Light+Human+Air+Parachutist",
)

cdef tuple PARENTED_TYPES = ("Weapon", "Projectile", "Decoy", "Container", "Flare")


cdef class ObjectRec:
    """Dataclass for objects."""
    cdef public object impacted, parent, Color, Coalition, Name, Country, grp, Pilot, Type
    cdef public int id, tac_id, session_id, updates
    cdef public float first_seen, last_seen, lat, lon, alt, roll, pitch, yaw, u_coord, v_coord, heading, impacted_dist, parent_dist, velocity_kts, secs_since_last_seen
    cdef public bint alive, written, can_be_parent, should_have_parent, is_air, is_ground, is_weapon
    cdef public list cart_coords

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
        self.is_weapon = False
        self.is_ground = False
        self.is_air = False


cdef ObjectRec set_obj_class(ObjectRec rec):
    if rec.Type in ['Weapon+Missile', 'Weapon+Bomb', 'Projectile+Shell']:
        rec.is_weapon = True
    elif rec.Type in  ["Ground+AntiAircraft", "Ground+Heavy+Armor+Vehicle+Tank",
        "Ground+Vehicle", "Ground+Static+Building", "Ground+Light+Human+Infantry"]:
        rec.is_ground = True
    elif rec.Type in ["Air+FixedWing", "Air+Rotorcraft"]:
        rec.is_air = True
    else:
        pass
    return rec


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


cpdef np.ndarray compute_dist_arr(np.ndarray p_1, np.ndarray p_2):
    """Compute cartesian distance between points."""
    return np.sqrt(
        (p_2[:,0] - p_1[0]) ** 2 + (p_2[:,1] - p_1[1]) ** 2 + (p_2[:,2] - p_1[2]) ** 2
    )



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


cpdef ObjectRec proc_line(bytearray raw_line, double ref_lat,
                     double ref_lon, dict obj_store,
                     float time_offset, int session_id):
    """Parse a textline from tacview into an ObjectRec."""

    comma = raw_line.find(b",")
    rec_id = int(raw_line[0:comma], 16)
    try:
        # Make update to existing record
        rec = obj_store[rec_id]
        rec.secs_since_last_seen = time_offset - rec.last_seen
        rec.last_seen = time_offset
        rec.updates += 1

    except KeyError:
        # Object not yet seen...create new record...
        rec = ObjectRec(
            tac_id=rec_id,
            session_id=session_id,
            first_seen=time_offset,
            last_seen=time_offset,
        )
        obj_store[rec_id] = rec

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
        rec = set_obj_class(rec)
        rec.can_be_parent = can_be_parent(rec.Type)
        rec.should_have_parent = should_have_parent(rec.Type)

    rec = compute_velocity(rec)

    if rec.updates == 1 and rec.should_have_parent:
        parent_info = determine_contact(rec,  obj_store, contact_type=2)
        if parent_info:
            rec.parent = parent_info[0]
            rec.parent_dist = parent_info[1]

    return rec # [rec, obj_store]


cdef bint can_be_parent(str rec_type):
    """Check if an object is a member of types that could be parents."""
    for t in NON_PARENTED_TYPES:
        if t in rec_type:
            return False
    else:
        return True


cdef bint should_have_parent(str rec_type):
    """Check if an object should have a parent record."""
    for t in PARENTED_TYPES:
        if t in rec_type:
            return True
    return False


cpdef list determine_contact(ObjectRec rec, dict obj_store, int contact_type):
    """Determine the parent of missiles, rockets, and bombs."""
    cdef tuple acpt_colors
    # 1 = impacted
    # 2 = parent
    if contact_type == 1:
        if not (rec.should_have_parent and rec.is_weapon == True):
            return
        acpt_colors = tuple(["Red"]) if rec.Color == "Blue" else tuple(["Blue"])

    else:
        if rec.Color == "Violet":
            acpt_colors = ("Red", "Blue", "Grey")
        else:
            acpt_colors = tuple([rec.Color])

    cdef list closest = []
    cdef list possible_coords = []
    cdef list possible_ids = []
    n_checked = 0
    offset_time = rec.last_seen - 2.5

    for near in obj_store.values():

        if (near.can_be_parent == False
            or near.tac_id == rec.tac_id
            or near.Color not in acpt_colors
            or (contact_type == 1 and near.is_air == False)
            or (offset_time > near.last_seen and (
                not near.is_ground == True
                and near.alive == True)
        )):
            continue

        n_checked += 1
        possible_coords.append(near.cart_coords)
        possible_ids.append(near.id)

        # prox = compute_dist(rec.cart_coords, near.cart_coords)
        # if not closest or (prox < closest[1]):
        #     closest = [near.id, prox]

    if not possible_coords:
        return

    cdef np.ndarray prox_arr = compute_dist_arr(np.array(rec.cart_coords), np.array(possible_coords))
    cdef int prox_idx = prox_arr.argmin()
    closest = [possible_ids[prox_idx], prox_arr[prox_idx]]

    if closest[1] > 200 and contact_type == 2:
        return

    return closest