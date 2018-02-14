# coding=utf-8
from __future__ import unicode_literals
import json

import mock
import pytest

from pg2kinesis.slot import PrimaryKeyMapItem
from pg2kinesis.formatter import Change, CSVFormatter, CSVPayloadFormatter, Formatter, get_formatter


def get_formatter_produce_formatted_message(cls):
    # type: (Formatter, unicode) -> Message
    change = Change(xid=1, table=u'public.blue', operation=u'Update', pkey=u'123456')
    result = cls({}).produce_formatted_message(change)
    assert result.change == change
    return result


def test_CSVFormatter_produce_formatted_message():
    result = get_formatter_produce_formatted_message(CSVFormatter)

    assert result.fmt_msg == u'0,CDC,1,public.blue,Update,123456'


def test_CSVPayloadFormatter_produce_formatted_message():
    result = get_formatter_produce_formatted_message(CSVPayloadFormatter)
    assert result.fmt_msg.startswith(u'0,CDC,')
    payload = result.fmt_msg.split(',', 2)[-1]
    assert json.loads(payload) == dict(xid=1, table=u'public.blue', operation=u'Update', pkey=u'123456')


@pytest.fixture
def pkey_map():
    return {'public.test_table': PrimaryKeyMapItem(u'public.test_table', u'uuid', u'uuid', 0),
            'public.test_table2': PrimaryKeyMapItem(u'public.test_table2', u'name', u'character varying', 0)}


@pytest.fixture(params=[CSVFormatter, CSVPayloadFormatter, Formatter])
def formatter(request, pkey_map):
    return request.param(pkey_map)


def test___init__(formatter):
    # processes primary key map (adds colon)

    patterns = formatter._primary_key_patterns

    assert len(patterns) == 2
    assert u'public.test_table:' in patterns, 'with colon'
    assert u'public.test_table2:' in patterns, 'with colon'
    assert u'public.test_table' not in patterns, 'without colon should not be in patterns'
    assert u'public.test_table2' not in patterns, 'without colon should not be in patterns'
    assert patterns[u'public.test_table:'].pattern == ur"uuid\[uuid\]:'?([\w\-]+)'?"
    assert patterns[u'public.test_table2:'].pattern == ur"name\[character varying\]:'?([\w\-]+)'?"


def test__preprocess_test_decoding_change(formatter):
    # assert begin -> None + cur trans
    assert formatter.cur_xact == ''
    result = formatter._preprocess_test_decoding_change(u'BEGIN 100')
    assert result == []
    assert formatter.cur_xact == u'100'

    # assert commit -> None
    formatter.cur_xact = ''
    result = formatter._preprocess_test_decoding_change(u'COMMIT')
    assert result == []
    assert formatter.cur_xact == ''

    # error states
    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_test_decoding_change(u'UNKNOWN BLING')
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unknown change: "UNKNOWN BLING"')

    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_test_decoding_change(u"table not_a_table: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a'")
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unable to locate table: "not_a_table:"')

    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_test_decoding_change(u"table public.test_table: UPDATE: not[not]:'00079f3e-0479-4475-acff-4f225cc5188a'")
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unable to locate primary key for table "public.test_table"')

    # assert on proper match
    formatter.cur_xact = '1337'
    change = formatter._preprocess_test_decoding_change(
        u"table public.test_table: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a'")[0]

    assert change.xid == u'1337'
    assert change.table == u'public.test_table'
    assert change.operation == u'UPDATE'
    assert change.pkey == u'00079f3e-0479-4475-acff-4f225cc5188a'

    change = formatter._preprocess_test_decoding_change(
        u"table public.test_table2: DELETE: name[character varying]:'Bling-2'")[0]

    assert change.xid == u'1337'
    assert change.table == u'public.test_table2'
    assert change.operation == u'DELETE'
    assert change.pkey == u'Bling-2'


def test__preprocess_wal2json_change(formatter):
    formatter.cur_xact = ''
    result = formatter._preprocess_wal2json_change(u"""{
                "xid": 101,
                "change": []
            }""")
    assert result == []
    assert formatter.cur_xact == 101

    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_wal2json_change(u"""{
                "xid": 100,
                "change": [
                    {
                        "kind": "insert",
                        "schema": "public",
                        "table": "not_a_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
                ]
            }""")
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unable to locate table: "public.not_a_table"')

    # assert on proper match
    formatter.cur_xact = '1337'
    change = formatter._preprocess_wal2json_change(u"""{
                "xid": 1337,
                "change": [
                    {
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
                ]
            }""")[0]

    assert change.xid == 1337
    assert change.table == u'public.test_table'
    assert change.operation == u'insert'
    assert change.pkey == u'00079f3e-0479-4475-acff-4f225cc5188a'

    change = formatter._preprocess_wal2json_change(u"""{
                "xid": 1337,
                "change": [
                    {
                        "kind": "delete",
                        "schema": "public",
                        "table": "test_table2",
                        "columnnames": ["name"],
                        "columntypes": ["varchar"],
                        "columnvalues": ["Bling-2"]
                    }
                ]
            }""")[0]

    assert change.xid == 1337
    assert change.table == u'public.test_table2'
    assert change.operation == u'delete'
    assert change.pkey == u'Bling-2'


def test__preprocess_wal2json_full_change(formatter):
    formatter.cur_xact = ''
    formatter.full_change = True

    result = formatter._preprocess_wal2json_change(u"""{
                "xid": 101,
                "change": []
            }""")
    assert result == []
    assert formatter.cur_xact == 101

    # Full changes from wal2json are not validated against known table map
    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_wal2json_change(u"""{
                "xid": 100,
                "change": [
                    {
                        "kind": "insert",
                        "schema": "public",
                        "table": "not_a_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
                ]
            }""")
        assert not mock_log_and_raise.called

    # assert on proper match
    formatter.cur_xact = '1337'
    change = formatter._preprocess_wal2json_change(u"""{
                "xid": 1337,
                "change": [
                    {
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
                ]
            }""")[0]

    assert change.xid == 1337
    assert change.change == {
        "kind": "insert",
        "schema": "public",
        "table": "test_table",
        "columnnames": ["uuid"],
        "columntypes": ["int4"],
        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
    }

    change = formatter._preprocess_wal2json_change(u"""{
                "xid": 1337,
                "change": [
                    {
                        "kind": "delete",
                        "schema": "public",
                        "table": "test_table2",
                        "columnnames": ["name"],
                        "columntypes": ["varchar"],
                        "columnvalues": ["Bling-2"]
                    }
                ]
            }""")[0]

    assert change.xid == 1337
    assert change.change == {
        "kind": "delete",
        "schema": "public",
        "table": "test_table2",
        "columnnames": ["name"],
        "columntypes": ["varchar"],
        "columnvalues": ["Bling-2"]
    }


def test_log_and_raise(formatter):

    with mock.patch('logging.Logger.error') as mock_log, pytest.raises(Exception) as e_info:
        formatter._log_and_raise(u'HELP!')

    assert e_info.value.message == u'HELP!'
    mock_log.assert_called_with(u'HELP!')


def test___call__(formatter):
    with mock.patch.object(formatter, '_preprocess_test_decoding_change', return_value=[]) as mock_preprocess_test_decoding_change, \
            mock.patch.object(formatter, 'produce_formatted_message', return_value=None) as mock_pfm:
        assert not mock_preprocess_test_decoding_change.called
        result = formatter('COMMIT')
        assert mock_preprocess_test_decoding_change.called
        assert result == []
        assert not mock_pfm.called

    with mock.patch.object(formatter, '_preprocess_test_decoding_change', return_value=['blue']) as mock_preprocess_test_decoding_change, \
            mock.patch.object(formatter, 'produce_formatted_message', return_value='blue msg') as mock_pfm:
        assert not mock_preprocess_test_decoding_change.called
        result = formatter('blue message')
        assert mock_preprocess_test_decoding_change.called
        assert result == ['blue msg']
        mock_pfm.assert_called_with('blue')


def test_get_formatter():
    with mock.patch.object(Formatter, '__init__', return_value=None) as mocked:
        result = get_formatter('CSVPayload', 1, 2, 3, 4)
        assert isinstance(result, CSVPayloadFormatter)
        assert mocked.called
        mocked.assert_called_with(1, 2, 3, 4)

    with mock.patch.object(Formatter, '__init__', return_value=None) as mocked:
        result = get_formatter('CSV', 1, 2, 3, 4)
        assert isinstance(result, CSVFormatter)
        assert mocked.called
        mocked.assert_called_with(1, 2, 3, 4)
