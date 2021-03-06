"""Test tacview parser."""
import asyncio
import pytest

# import sys
# sys.path.append("..")
from tacview_client import __version__
from tacview_client.client import Ref
from tacview_client.cython_funs import cyfuns

# pytestmark = pytest.mark.asyncio


@pytest.fixture
@pytest.mark.asyncio
async def ref_obj():
    """Fixture to generate a database and ref."""
    ref = Ref()
    await ref.parse_ref_obj(b"ReferenceLatitude=0.0")
    await ref.parse_ref_obj(b"ReferenceLongitude=0.0")
    await ref.parse_ref_obj(b"DataSource=Mission")
    await ref.parse_ref_obj(b"Title=GoodMission")
    await ref.parse_ref_obj(b"Author=Bob")
    await ref.parse_ref_obj(b"RecordingTime=2019-01-01T12:12:01.101Z")
    ref.update_time(b"#1.01")
    return ref


@pytest.mark.asyncio
async def test_update_string(ref_obj):
    """Test that update strings are parsed properly."""
    new_string = bytearray(
        b"802,T=6.3596289|5.139203|342.67|||7.3|729234.25|-58312.28|,"
        b"Type=Ground+Static+Aerodrome,Name=FARP,Color=Blue,"
        b"Coalition=Enemies,Country=us"
    )
    cyfuns.proc_line(raw_line=new_string, ref=ref_obj)

    update_string = bytearray(b"802,T=123.45|678.09|234.2||")
    correct_resp = {"id": int("802", 16), "lat": 678.09, "lon": 123.45, "alt": 234.2}
    parsed = cyfuns.proc_line(raw_line=update_string, ref=ref_obj)
    for key, value in correct_resp.items():
        if key == "id":
            continue
        assert value == getattr(parsed, key)


@pytest.mark.asyncio
async def test_new_entry_without_alt(ref_obj):
    """Test that a new record with no altitude is assigned 1.0."""
    input_bytes = bytearray(
        b"4001,T=4.6361975|6.5404775||||357.8|-347259.72|380887.44|,"
        b"Type=Ground+Heavy+Armor+Vehicle+Tank,Name=BTR-80,"
        b"Group=New Vehicle Group #041,Color=Red,Coalition=Enemies,Country=ru"
    )
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)
    assert parsed.alt == 1.0


@pytest.mark.asyncio
async def test_negative_integer_alt(ref_obj):
    input_bytes = bytearray(
        b"4001,T=4.6361975|6.5404775||||357.8|-347259.72|380887.44|,"
        b"Type=Ground+Heavy+Armor+Vehicle+Tank,Name=BTR-80,"
        b"Group=New Vehicle Group #041,Color=Red,Coalition=Enemies,Country=ru"
    )
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)

    input_bytes = bytearray(b"4001,T=6.96369|4.0232604|-2||")
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)
    assert parsed.alt == -2.0


@pytest.mark.asyncio
async def test_u_coord_error(ref_obj):
    input_bytes = bytearray(
        b"76502,T=4.6361975|6.5404775||||357.8|-347259.72|380887.44|,"
        b"Type=Ground+Heavy+Armor+Vehicle+Tank,Name=BTR-80,"
        b"Group=New Vehicle Group #041,Color=Red,Coalition=Enemies,Country=ru"
    )
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)

    input_bytes = bytearray(b"76502,T=6.6632117|4.8577435|6640.74|-57047.37|76446.19")
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)
    assert parsed.v_coord == 76446.19
    assert parsed.alt == 6640.74

    input_bytes = bytearray(b"76502,T=5.8429775|4.1803246|7224.58|-139814.45|2511.76")
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)
    assert parsed.v_coord == 2511.76
    assert parsed.u_coord == -139814.45
    assert parsed.alt == 7224.58


@pytest.mark.asyncio
async def test_line_parser(ref_obj):
    """Test that update strings are parsed properly."""
    input_bytes = bytearray(
        b"802,T=6.3596289|5.139203|342.67|||7.3|729234.25|-58312.28|,"
        b"Type=Ground+Static+Aerodrome,Name=FARP,Color=Blue,"
        b"Coalition=Enemies,Country=us"
    )
    parsed = cyfuns.proc_line(input_bytes, ref_obj.lat, ref_obj.lon, ref_obj.obj_store,
                              ref_obj.time_offset, ref_obj.session_id)

    correct_resp = {
        "tac_id": int(b"802", 16),
        "lat": 5.139203,
        "lon": 6.3596289,
        "alt": 342.67,
        "Type": "Ground+Static+Aerodrome",
        "Name": "FARP",
        "Color": "Blue",
        "Coalition": "Enemies",
        "Country": "us",
    }

    for key, value in correct_resp.items():
        assert value == getattr(parsed, key)
