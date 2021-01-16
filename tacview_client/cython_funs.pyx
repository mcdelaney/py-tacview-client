from libc.math cimport sqrt
from libc.math cimport cos
from libc.math cimport sin
from libc.math cimport M_PI

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


cpdef list compute_velocity(double lat, double lon, double alt, list cart_coords, double secs_since_last_seen):
        """Calculate velocity given the distance from the last point."""
        cdef list new_cart_coords = get_cartesian_coord(lat, lon, alt)
        cdef double velocity_kts
        cdef double t_dist
        if (
            cart_coords
            and secs_since_last_seen
            and secs_since_last_seen > 0.0
        ):
            t_dist = compute_dist(new_cart_coords, cart_coords)
            velocity_kts = (t_dist / secs_since_last_seen) / 1.94384
        return [new_cart_coords, velocity_kts]


cpdef dict proc_line(bytes raw_line, double ref_lat, double ref_lon):
    """Parse a textline from tacview into an ObjectRec."""
    cdef list COORD_KEYS = (
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

    cdef list COORD_KEYS_SHORT = ("lon", "lat", "alt", "u_coord", "v_coord")
    cdef list COORD_KEY_SHORT_LEN = 5

    cdef list COORD_KEYS_MED = ("lon", "lat", "alt", "roll", "pitch", "yaw")
    cdef int COORD_KEYS_MED_LEN = 5

    cdef list COORD_KEYS_X_SHORT = ("lon", "lat", "alt")
    cdef int COORD_KEYS_X_SHORT_LEN = 3

    cdef bool bytes_remaining = True

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
    return rec