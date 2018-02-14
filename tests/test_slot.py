from mock import call, Mock, MagicMock, patch, PropertyMock

import pytest
import psycopg2
import psycopg2.errorcodes

from pg2kinesis.slot import SlotReader


@pytest.fixture
def slot():
    slot = SlotReader('blah_db', 'blah_host', 'blah_port', 'blah_user', 'pg2kinesis', keepalive_window=0)
    slot._repl_cursor = Mock()
    slot._repl_conn = Mock()
    slot._normal_conn = Mock()
    slot._keepalive_thread = Mock()

    return slot


def test__enter__(slot):
    # returns its self

    with patch.object(slot, '_get_connection', side_effects=[Mock(), Mock()]) as mock_gc, \
            patch.object(slot, '_send_keepalive') as mock_ka:
        assert slot == slot.__enter__(), 'Returns itself'
        assert mock_gc.call_count == 2

        assert call.set_isolation_level(0) in slot._normal_conn.method_calls, 'make sure we are in autocommit'
        assert call.cursor() in slot._repl_conn.method_calls, 'we opened a cursor'

        assert not mock_ka.called, "with no window we didn't start keep alive"

    slot._keepalive_window = 1
    with patch.object(slot, '_get_connection', side_effects=[Mock(), Mock()]) as mock_gc, \
            patch.object(slot, '_send_keepalive') as mock_ka:
        slot.__enter__()
        assert mock_ka.called, " we started up keepalive"


def test__exit__(slot):
    slot._keepalive_window = 10
    slot.__exit__(None, None, None)

    assert call.close() in slot._repl_cursor.method_calls
    assert call.close() in slot._repl_conn.method_calls
    assert call.close() in slot._normal_conn.method_calls
    assert call.join(timeout=13) in slot._keepalive_thread.method_calls

    slot._repl_cursor.close = Mock(side_effect=Exception)
    slot._repl_conn.close = Mock(side_effect=Exception)
    slot._normal_conn.close= Mock(side_effect=Exception)
    slot._keepalive_thread.join = Mock(side_effect=Exception)
    slot.__exit__(None, None, None)

    assert slot._keepalive_thread.join.called
    assert slot._repl_cursor.close.called, "Still called even thought call above raised"
    assert slot._repl_conn.close.called, "Still called even thought call above raised"
    assert slot._normal_conn.close.called, "Still called even thought call above raised"



def assert_stuff_about_keep_alive(slot, mock_timer, exception, logging_called):
    mock_timer.reset()
    slot._repl_cursor.reset()

    with patch('logging.Logger.exception') as mock_log:
        slot._repl_cursor.send_feedback = Mock(side_effect=exception)
        thread = slot._send_keepalive()

    assert mock_log.called == logging_called
    assert call.send_feedback() in slot._repl_cursor.method_calls
    assert mock_timer.called
    assert thread.daemon == True, 'It is a daemon'
    assert call.start() in thread.method_calls, 'It has been started'



def test__send_keep_alive(slot):
    with patch('threading.Timer') as mock_timer:
        # No matter what we schedule keep alive thread. Sometimes we log an error
        db_error = psycopg2.DatabaseError()
        db_error.message = 'Log ME!'
        assert_stuff_about_keep_alive(slot, mock_timer, db_error, True)

        db_error = psycopg2.DatabaseError()
        db_error.message = 'no COPY in progress\n'
        assert_stuff_about_keep_alive(slot, mock_timer, db_error, False)

        error = Exception()
        error.message = 'no COPY in progress\n'
        assert_stuff_about_keep_alive(slot, mock_timer, error, True)

        # Happy Path
        assert_stuff_about_keep_alive(slot, mock_timer, None, False)


def test_create_slot(slot):

    with patch.object(psycopg2.ProgrammingError, 'pgcode',
                      new_callable=PropertyMock,
                      return_value=psycopg2.errorcodes.DUPLICATE_OBJECT):
        pe = psycopg2.ProgrammingError()


        slot._repl_cursor.create_replication_slot = Mock(side_effect=pe)
        slot.create_slot()
        slot._repl_cursor.create_replication_slot.assert_called_with('pg2kinesis',
                                                                     slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                                     output_plugin=u'test_decoding')
    with patch.object(psycopg2.ProgrammingError, 'pgcode',
                          new_callable=PropertyMock,
                          return_value=-1):
        pe = psycopg2.ProgrammingError()
        slot._repl_cursor.create_replication_slot = Mock(side_effect=pe)

        with pytest.raises(psycopg2.ProgrammingError) as e_info:
            slot.create_slot()
            slot._repl_cursor.create_replication_slot.assert_called_with('pg2kinesis',
                                                                         slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                                         output_plugin=u'test_decoding')
        assert e_info.value.pgcode == -1

        slot._repl_cursor.create_replication_slot = Mock(side_effect=Exception)
    with pytest.raises(Exception):
        slot.create_slot()
        slot._repl_cursor.create_replication_slot.assert_called_with('pg2kinesis',
                                                                         slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                                         output_plugin=u'test_decoding')


def test_delete_slot(slot):
    with patch.object(psycopg2.ProgrammingError, 'pgcode',
                      new_callable=PropertyMock,
                      return_value=psycopg2.errorcodes.UNDEFINED_OBJECT):
        pe = psycopg2.ProgrammingError()
        slot._repl_cursor.drop_replication_slot = Mock(side_effect=pe)
        slot.delete_slot()
    slot._repl_cursor.drop_replication_slot.assert_called_with('pg2kinesis')

    with patch.object(psycopg2.ProgrammingError, 'pgcode',
                      new_callable=PropertyMock,
                      return_value=-1):
        pe = psycopg2.ProgrammingError()
        slot._repl_cursor.create_replication_slot = Mock(side_effect=pe)
        with pytest.raises(psycopg2.ProgrammingError) as e_info:
            slot.delete_slot()
            slot._repl_cursor.drop_replication_slot.assert_called_with('pg2kinesis')

            assert e_info.value.pgcode == -1

    slot._repl_cursor.create_replication_slot = Mock(side_effect=Exception)
    with pytest.raises(Exception):
        slot.delete_slot()
        slot._repl_cursor.drop_replication_slot.assert_called_with('pg2kinesis')


def test__get_connection(slot):
    with patch('psycopg2.connect') as mock_connect:
        slot._get_connection()
        kall = call(connection_factory=None, cursor_factory=None, database='blah_db', host='blah_host',
                    port='blah_port', user='blah_user')
        assert mock_connect.called_with(kall)

        slot._get_connection(connection_factory='connection_fact', cursor_factory='cursor_fact')
        kall = call(connection_factory='connection_fact', cursor_factory='cursor_fact', database='blah_db', host='blah_host',
                    port='blah_port', user = 'blah_user')
        assert mock_connect.called_with(kall)


def test_primary_key_map(slot):
    slot._execute_and_fetch = Mock(return_value=[('test_table', 'pkey', 'uuid', 0),
                                                 ('test_table2', 'pkey', 'uuid', 0),
                                                 ('blue', 'bkey', 'char var', 10)
                                                 ])

    pkey_map = slot.primary_key_map

    assert len(pkey_map) == 3
    assert 'test_table' in pkey_map
    assert 'test_table2' in pkey_map
    assert 'blue' in pkey_map

    assert pkey_map['blue'].table_name == 'blue'
    assert pkey_map['blue'].col_name == 'bkey'
    assert pkey_map['blue'].col_type == 'char var'
    assert pkey_map['blue'].col_ord_pos == 10


def test_execute_and_fetch(slot):
    norm_conn = slot._normal_conn
    mock_cur = MagicMock()
    norm_conn.cursor = Mock(return_value=mock_cur)

    slot._execute_and_fetch('SQL SQL STATEMENT', 1, 2, 3)
    call.execute('SQL SQL STATEMENT', (1, 2, 3)) in mock_cur.method_calls
    assert call.__enter__().fetchall() in mock_cur.mock_calls

    mock_cur.reset_mock()
    slot._execute_and_fetch('SQL SQL STATEMENT')
    call.execute('SQL SQL STATEMENT') in mock_cur.method_calls
    assert call.__enter__().fetchall() in mock_cur.mock_calls


def test_process_replication_stream(slot):
    consume = Mock()
    slot.process_replication_stream(consume)

    assert call.start_replication('pg2kinesis', options=None) in  slot._repl_cursor.method_calls, 'We started replication event loop'
    assert call.consume_stream(consume) in slot._repl_cursor.method_calls, 'We pass consume to this method'

