from __future__ import unicode_literals

from mock import Mock, call

from pg2kinesis.__main__ import Consume

def test_consume():
    mock_formatter = Mock(return_value='fmt_msg')
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