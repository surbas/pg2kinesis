from __future__ import unicode_literals

from mock import Mock, call, patch

from pg2kinesis.__main__ import Consume

def test_consume():
    mock_formatter = Mock(return_value='fmt_msg')
    # required to avoid formatting error if cur_xact is logged
    mock_formatter.cur_xact = 'TEST_TRANSACTION'
    mock_writer = Mock()

    consume = Consume(mock_formatter, mock_writer)

    mock_change = Mock()
    mock_change.data_start = 10
    mock_change.data_size = 100
    mock_change.payload = 'PAYLOAD'

    mock_writer.put_message = Mock(return_value=False)
    consume(mock_change)
    assert mock_writer.put_message.called, 'Sanity'
    assert call.cursor.send_feedback(flush_lsn=10) not in mock_change.mock_calls, \
        'we did not send feedback!'

    mock_writer.put_message = Mock(return_value=True)
    consume(mock_change)
    assert mock_writer.put_message.called, 'Sanity'
    assert call.cursor.send_feedback(flush_lsn=10) in mock_change.mock_calls, \
        'we sent feedback!'


    mock_time = Mock()
    mock_time.return_value = 11.0

    consume.msg_window_size = 0
    consume.msg_window_count = 0
    consume.cur_window = 10
    with patch('time.time', mock_time):
        consume(mock_change)
        assert consume.cur_window == 10, 'cur window not updated if time is non-10-multiple'
        assert consume.msg_window_size == 100, 'msg_window_size not reset if time is non-10-multiple'
        assert consume.msg_window_count == 1, 'msg_window_count not reset if time is non-10-multiple'

    mock_time.return_value = 20.0
    with patch('time.time', mock_time):
        consume(mock_change)
        assert consume.cur_window == 20, 'cur window updated if time is multiple of 10'
        assert consume.msg_window_size == 0, 'msg_window_size reset if time is multiple of 10'
        assert consume.msg_window_count == 0, 'msg_window_count reset if time is multiple of 10'

    with patch('time.time', mock_time):
        consume(mock_change)
        assert consume.msg_window_size == 100, 'msg_window_size not reset if time is same as cur_window'
