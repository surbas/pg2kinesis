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
    return {'test_table': PrimaryKeyMapItem(u'test_table', u'uuid', u'uuid', 0),
            'test_table2': PrimaryKeyMapItem(u'test_table2', u'name', u'character varying', 0)}


@pytest.fixture(params=[CSVFormatter, CSVPayloadFormatter, Formatter])
def formatter(request, pkey_map):
    return request.param(pkey_map)


def test___init__(formatter):
    # processes primary key map (adds colon)

    patterns = formatter._primary_key_patterns

    assert len(patterns) == 2
    assert u'test_table:' in patterns, 'with colon'
    assert u'test_table2:' in patterns, 'with colon'
    assert u'test_table' not in patterns, 'without colon should not be in patterns'
    assert u'test_table2' not in patterns, 'without colon should not be in patterns'
    assert patterns[u'test_table:'].pattern == ur"uuid\[uuid\]:'?([\w\-]+)'?"
    assert patterns[u'test_table2:'].pattern == ur"name\[character varying\]:'?([\w\-]+)'?"


def test__preprocess_change(formatter):
    # assert begin -> None + cur trans
    assert formatter.cur_xact == ''
    result = formatter._preprocess_change(u'BEGIN 100')
    assert result is None
    assert formatter.cur_xact == u'100'

    # assert commit -> None
    formatter.cur_xact = ''
    result = formatter._preprocess_change(u'COMMIT')
    assert result is None
    assert formatter.cur_xact == ''

    # error states
    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_change(u'UNKNOWN BLING')
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unknown change: "UNKNOWN BLING"')

    with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
        formatter._preprocess_change(u"table not_a_table: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a'")
        assert mock_log_and_raise.called
        mock_log_and_raise.assert_called_with(u'Unable to locate table: "not_a_table:"')

    # TODO: Disabled for now
    # with mock.patch.object(formatter, '_log_and_raise') as mock_log_and_raise:
    #     formatter._preprocess_change(u"table test_table: UPDATE: not[not]:'00079f3e-0479-4475-acff-4f225cc5188a'")
    #     assert mock_log_and_raise.called
    #     mock_log_and_raise.assert_called_with(u'Unable to locate primary key for table "test_table"')

    # assert on proper match
    formatter.cur_xact = '1337'
    change = formatter._preprocess_change(
        u"table test_table: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a'")

    assert change.xid == u'1337'
    assert change.table == u'test_table'
    assert change.operation == u'UPDATE'
    assert change.pkey == u'00079f3e-0479-4475-acff-4f225cc5188a'

    change = formatter._preprocess_change(
        u"table test_table2: DELETE: name[character varying]:'Bling-2'")

    assert change.xid == u'1337'
    assert change.table == u'test_table2'
    assert change.operation == u'DELETE'
    assert change.pkey == u'Bling-2'


def test_log_and_raise(formatter):

    with mock.patch('logging.Logger.error') as mock_log, pytest.raises(Exception) as e_info:
        formatter._log_and_raise(u'HELP!')

    assert e_info.value.message == u'HELP!'
    mock_log.assert_called_with(u'HELP!')


def test___call__(formatter):
    with mock.patch.object(formatter, '_preprocess_change', return_value=None) as mock_preprocess_change, \
            mock.patch.object(formatter, 'produce_formatted_message', return_value=None) as mock_pfm:
        assert not mock_preprocess_change.called
        result = formatter('fake message')
        assert mock_preprocess_change.called
        assert result is None
        assert not mock_pfm.called

    with mock.patch.object(formatter, '_preprocess_change', return_value='blue') as mock_preprocess_change, \
            mock.patch.object(formatter, 'produce_formatted_message', return_value='blue msg') as mock_pfm:
        assert not mock_preprocess_change.called
        result = formatter('blue message')
        assert mock_preprocess_change.called
        assert result == 'blue msg'
        mock_pfm.assert_called_with('blue')


def test_get_formatter():
    with mock.patch.object(Formatter, '__init__', return_value=None) as mocked:
        result = get_formatter('CSVPayload', 1, 2, 3)
        assert isinstance(result, CSVPayloadFormatter)
        assert mocked.called
        mocked.assert_called_with(1, 2, 3)

    with mock.patch.object(Formatter, '__init__', return_value=None) as mocked:
        result = get_formatter('CSV', 1, 2, 3)
        assert isinstance(result, CSVFormatter)
        assert mocked.called
        mocked.assert_called_with(1, 2, 3)
