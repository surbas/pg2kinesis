import pytest

from ..slot import SlotReader


@pytest.fixture
def slot():
    return SlotReader('blah_db', 'blah_host', 'blah_port', 'blah_user', 'pg2kinesis')

def test__enter__(slot):
    # returns its self



    pass

def test__exit__(slot):
    # cleans it self up
    pass

def _keep_alive(slot):
    pass

def test_create_slot(slot):
    pass

def test_drop_slot(slot):
    pass

def test__get_connection(slot):
    pass

def test_primary_key_map(slot):
    pass

def test_execute_and_fetch(slot):
    pass

def test_process_replication_stream(slot):
    pass
