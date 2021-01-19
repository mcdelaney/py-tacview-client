from libc.math cimport sqrt
from libc.math cimport cos
from libc.math cimport sin
from libc.math cimport M_PI

import pytz
import numpy as np
from datetime import datetime
cimport numpy as np
np.import_array()

from tacview_client.config import DB_URL
from tacview_client import __version__

# ctypedef np.int_t DTYPE_t
from cython.parallel import prange

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
cdef int COORD_KEYS_MED_LEN = 6

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

cdef dict obj_store = {}


cdef class Ref:
    """Hold and extract Reference values used as offsets."""
    cdef public int session_id
    cdef public float file_version, time_offset, time_since_last
    cdef public float lat, lon
    cdef public str title, datasource, author, client_version, status
    cdef public object start_time
    cdef public bint all_refs
    cdef public dict obj_store

    def __init__(self):
        self.session_id = 0
        self.lat = 0.0
        self.lon = 0.0
        self.title = None
        self.datasource
        self.file_version = 0.0
        self.author = None
        self.start_time = None
        self.time_offset = 0.0
        self.all_refs = False
        self.time_since_last = 0.0
        self.obj_store = {}
        self.client_version = __version__
        self.status = "In Progress"

    def update_time(self, str line):
        """Update the refence time attribute with a new offset."""
        cdef float offset
        offset = float(line[1:])
        self.time_since_last = offset - self.time_offset
        self.time_offset = offset

    def parse_ref_obj(self, str line):
        """
        Attempt to extract ReferenceLatitude, ReferenceLongitude or
        ReferenceTime from a line object.
        """
        cdef list val
        try:
            val = line.split(",")[-1].split("=")

            if val[0] == "ReferenceLatitude":
                # LOG.debug("Ref latitude found...")
                self.lat = float(val[1])

            elif val[0] == "ReferenceLongitude":
                # LOG.debug("Ref longitude found...")
                self.lon = float(val[1])

            elif val[0] == "DataSource":
                # LOG.debug("Ref datasource found...")
                self.datasource = val[1]

            elif val[0] == "Title":
                # LOG.debug("Ref Title found...")
                self.title = val[1]

            elif val[0] == "Author":
                # LOG.debug("Ref Author found...")
                self.author = val[1]

            elif val[0] == "FileVersion":
                # LOG.debug("Ref Author found...")
                self.file_version = float(val[1])

            elif val[0] == "RecordingTime":
                # LOG.debug("Ref time found...")
                self.start_time = datetime.strptime(
                    val[1], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                self.start_time = self.start_time.replace(microsecond=0)
                self.start_time = self.start_time.replace(tzinfo=pytz.UTC)

            self.all_refs = bool(self.lat and self.lon and self.start_time)
        except IndexError:
            pass


cdef class ObjectRec:
    """Dataclass for objects."""
    cdef public object parent, impacted
    cdef public str Color, Coalition, Name, Country, Pilot, Type
    cdef public int id, tac_id, session_id, updates
    cdef public float first_seen, last_seen, roll, pitch, yaw, u_coord, v_coord, heading, impacted_dist, parent_dist
    cdef public float secs_since_last_seen, velocity_kts, lat, lon, alt
    cdef public bint alive, written, can_be_parent, should_have_parent, is_air, is_ground, is_weapon
    cdef public list cart_coords
    cdef public str grp

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
        self.secs_since_last_seen = 0.0
        self.written = False
        self.cart_coords = []
        self.is_weapon = False
        self.is_ground = False
        self.is_air = False

    property Group:
        def __set__(self, str value):
            self.grp = value


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


cpdef list get_cartesian_coord(float lat, float lon, float h):
    """Convert coords from geodesic to cartesian."""
    cdef list out = [0.0, 0.0, 0.0]
    cdef float a = 6378137.0
    cdef float rf = 298.257223563
    cdef float const = M_PI/180
    cdef float lat_rad = lat * const
    cdef float lon_rad = lon * const
    cdef float sin_lat_rad = sin(lat_rad)
    cdef float cos_lat_rad = cos(lat_rad)
    cdef float N = sqrt(a / (1 - (1 - (1 - 1 / rf) ** 2) * (sin_lat_rad) ** 2))

    cdef float intermed = (N + h) * cos_lat_rad
    out[0] = intermed * cos(lon_rad)
    out[1] = intermed * sin(lon_rad)
    out[2] = ((1 - 1 / rf) ** 2 * N + h) * sin_lat_rad
    return out


cpdef float compute_dist(list p_1, list p_2):
    """Compute cartesian distance between points."""
    # return np.sqrt(np.sum(np.square(p_2 - p_1), axis=0))
    cdef float result = sqrt(
        (p_2[0] - p_1[0]) ** 2 + (p_2[1] - p_1[1]) ** 2 + (p_2[2] - p_1[2]) ** 2
    )
    return result


cpdef np.ndarray compute_dist_arr(np.ndarray p_1, np.ndarray p_2):
    """Compute cartesian distance between points."""
    return np.sqrt(np.sum(np.square(p_2[:,] - p_1), axis = 1))


cpdef ObjectRec compute_velocity(ObjectRec rec):
    """Calculate velocity given the distance from the last point."""
    cdef list new_cart_coords = get_cartesian_coord(rec.lat, rec.lon, rec.alt)
    cdef float velocity_kts
    cdef float t_dist
    cdef float cnst = 1.94384
    if (
        rec.cart_coords and rec.secs_since_last_seen > 0.0
    ):
        t_dist = compute_dist(new_cart_coords, rec.cart_coords)
        velocity_kts = (t_dist / rec.secs_since_last_seen) / cnst

    rec.cart_coords = new_cart_coords
    if velocity_kts:
        rec.velocity_kts = velocity_kts

    return rec

cpdef list proc_line(str line, Ref ref):
    """Parse a textline from tacview into an ObjectRec."""
    # cdef str line = raw_line.decode("UTF-8")
    cdef ObjectRec rec
    cdef bint found_impact = False

    if line[0:1] == "0":
        return [None, found_impact]

    if line[0:1] == "-":
        # We know the Object is now dead
        rec = obj_store[int(line[1:], 16)]
        rec.alive = False
        rec.updates += 1

        impacted = determine_contact(rec, obj_store, contact_type=1)
        if impacted:
            rec.impacted = impacted[0]
            rec.impacted_dist = impacted[1]
            found_impact = True
        return [rec, found_impact]

    cdef list line_split = line.split(',')
    cdef int rec_id = int(line_split[0], 16)
    cdef list coords, split_eq
    cdef str coord, key, val, c_key
    cdef int npipe, i

    # try:
    if rec_id in obj_store.keys():
        # Make update to existing record
        rec = obj_store[rec_id]
        rec.secs_since_last_seen = ref.time_offset - rec.last_seen
        rec.last_seen = ref.time_offset
        rec.updates += 1

    # except KeyError:
    else:
        # Object not yet seen...create new record...
        rec = ObjectRec(
            tac_id=rec_id,
            session_id=ref.session_id,
            first_seen=ref.time_offset,
            last_seen=ref.time_offset,
        )
        obj_store[rec_id] = rec

    coords = line_split[1][2:].split("|")
    npipe = len(coords)
    if npipe == COORD_KEY_LEN:
        C_KEYS = COORD_KEYS
    elif npipe == COORD_KEYS_MED_LEN:
        C_KEYS = COORD_KEYS_MED
    elif npipe == COORD_KEY_SHORT_LEN:
        C_KEYS = COORD_KEYS_SHORT
    elif npipe == COORD_KEYS_X_SHORT_LEN:
        C_KEYS = COORD_KEYS_X_SHORT
    else:
        pass

    cdef int key_len = len(C_KEYS)
    for i in range(key_len):
        coord = coords[i]
        if not coord:
            continue
        if i == 0:
            rec.lon = float(coord) + ref.lon
        elif i == 1:
            rec.lat = float(coord) + ref.lat
        elif i == 1:
            rec.alt = float(coord)
        else:
            c_key = C_KEYS[i]
            setattr(rec, c_key, float(coord))

    for el in line_split[2:]:
        split_eq = el.split('=')
        key = split_eq[0]
        val = split_eq[1]
        setattr(rec, key, val)

    if rec.updates == 1:
        rec = set_obj_class(rec)
        can_be_parent(rec)
        should_have_parent(rec)

    rec = compute_velocity(rec)

    if rec.updates == 1 and rec.should_have_parent:
        parent_info = determine_contact(rec,  obj_store, contact_type=2)
        if parent_info:
            rec.parent = parent_info[0]
            rec.parent_dist = parent_info[1]

    return [rec, found_impact]


cdef can_be_parent(ObjectRec rec):
    """Check if an object is a member of types that could be parents."""
    if rec.is_weapon:
        return
    cdef str t
    for t in NON_PARENTED_TYPES:
        if t in rec.Type:
            return
    else:
        rec.can_be_parent = True


cdef should_have_parent(ObjectRec rec):
    """Check if an object should have a parent record."""
    if rec.is_weapon:
        rec.should_have_parent = True
        return
    cdef str t
    for t in PARENTED_TYPES:
        if t in rec.Type:
            rec.should_have_parent = True
            return


cpdef list determine_contact(ObjectRec rec, dict obj_store, int contact_type):
    """Determine the parent of missiles, rockets, and bombs."""
    # cdef tuple acpt_colors
    # 1 = impacted
    # 2 = parent
    if contact_type == 1:
        if not (rec.should_have_parent and rec.is_weapon == True):
            return
        # acpt_colors = tuple(["Red"]) if rec.Color == "Blue" else tuple(["Blue"])

    # else:
    #     if rec.Color == "Violet":
    #         acpt_colors = ("Red", "Blue", "Grey")
    #     else:
    #         acpt_colors = tuple([rec.Color])
    # cdef vector[int] possible_ids
    cdef list closest = []
    cdef list possible_coords = []
    cdef list possible_ids = []
    offset_time = rec.last_seen - 2.5

    cdef ObjectRec near
    for near in obj_store.values():
        if (near.can_be_parent == False
            # or near.Color not in acpt_colors
            or (contact_type == 1 and near.is_air == False)
            or (offset_time > near.last_seen and (
                not near.is_ground == True
                and near.alive == True)
            or near.tac_id == rec.tac_id
        )):
            continue

        possible_coords.append(near.cart_coords)
        possible_ids.append(near.id)

    if not possible_coords:
        return

    cdef np.ndarray prox_arr = compute_dist_arr(np.array(rec.cart_coords, dtype='float'), np.array(possible_coords, dtype='float'))
    cdef int prox_idx = prox_arr.argmin()
    closest = [possible_ids[prox_idx], prox_arr[prox_idx]]

    if closest[1] > 200 and contact_type == 2:
        return

    return closest