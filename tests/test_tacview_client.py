"""Test tacview parser."""
import pytest
from tacview_client.client import line_to_obj, Ref



@pytest.fixture
def ref_obj():
    """Fixture to generate a database and ref."""
    ref = Ref()
    ref.parse_ref_obj(b"ReferenceLatitude=0.0")
    ref.parse_ref_obj(b"ReferenceLongitude=0.0")
    ref.parse_ref_obj(b"DataSource=Mission")
    ref.parse_ref_obj(b"Title=GoodMission")
    ref.parse_ref_obj(b"Author=Bob")
    ref.parse_ref_obj(b"RecordingTime=2019-01-01T12:12:01.101Z")
    ref.update_time(b"#1.01")
    return ref


def test_update_string(ref_obj):
    """Test that update strings are parsed properly."""
    new_string = bytearray(b"802,T=6.3596289|5.139203|342.67|||7.3|729234.25|-58312.28|,"
                           b"Type=Ground+Static+Aerodrome,Name=FARP,Color=Blue,"
                           b"Coalition=Enemies,Country=us")
    parsed_orig = line_to_obj(raw_line=new_string, ref=ref_obj)

    update_string = bytearray(b"802,T=123.45|678.09|234.2||")
    correct_resp = {'id': int('802', 16),
                    'lat': 678.09,
                    'lon': 123.45,
                    'alt': 234.2
                    }
    parsed = line_to_obj(raw_line=update_string, ref=ref_obj)
    print(parsed.lat)
    print(parsed.lon)
    for key, value in correct_resp.items():
        if key == 'id':
            continue
        assert value == getattr(parsed, key)


def test_new_entry_without_alt(ref_obj):
    """Test that a new record with no altitude is assigned 1.0."""
    input_bytes = bytearray(b"4001,T=4.6361975|6.5404775|||357.8|-347259.72|380887.44|,"
                           b"Type=Ground+Heavy+Armor+Vehicle+Tank,Name=BTR-80,"
                           b"Group=New Vehicle Group #041,Color=Red,Coalition=Enemies,Country=ru")
    parsed = line_to_obj(raw_line=input_bytes, ref=ref_obj)
    assert parsed.alt == 1.0


def test_negative_integer_alt(ref_obj):
    input_bytes = bytearray(b"4001,T=4.6361975|6.5404775|||357.8|-347259.72|380887.44|,"
                           b"Type=Ground+Heavy+Armor+Vehicle+Tank,Name=BTR-80,"
                           b"Group=New Vehicle Group #041,Color=Red,Coalition=Enemies,Country=ru")
    parsed = line_to_obj(raw_line=input_bytes, ref=ref_obj)

    input_bytes = bytearray(b"4001,T=6.96369|4.0232604|-2")
    parsed = line_to_obj(raw_line=input_bytes, ref=ref_obj)
    assert parsed.alt == -2.0


def test_line_parser(ref_obj):
    """Test that update strings are parsed properly."""
    input_bytes = bytearray(b"802,T=6.3596289|5.139203|342.67|||7.3|729234.25|-58312.28|,"
                           b"Type=Ground+Static+Aerodrome,Name=FARP,Color=Blue,"
                           b"Coalition=Enemies,Country=us")
    parsed = line_to_obj(raw_line=input_bytes, ref=ref_obj)

    correct_resp = {'tac_id': int(b'802', 16),
                    'lat': 5.139203,
                    'lon': 6.3596289,
                    'alt': 342.67,
                    'Type': "Ground+Static+Aerodrome",
                    'Name': "FARP",
                    'Color': "Blue",
                    'Coalition': "Enemies",
                    'Country': "us",
                    }

    for key, value in correct_resp.items():
        assert value == getattr(parsed, key)
