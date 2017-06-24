import time

from freezegun import freeze_time
from mock import patch, call, Mock
import pytest
import boto3
from botocore.exceptions import ClientError

from pg2kinesis.stream import StreamWriter

@pytest.fixture()
def writer():
    with patch('aws_kinesis_agg.aggregator.RecordAggregator'), patch.object(boto3, 'client'):
        writer = StreamWriter('blah')
    return writer

def test__init__():
    mock_client = Mock()
    with patch.object(boto3, 'client', return_value=mock_client):
        error_response = {'Error': {'Code': 'ResourceInUseException'}}
        mock_client.create_stream = Mock(side_effect=ClientError(error_response, 'create_stream'))

        StreamWriter('blah')
        assert mock_client.create_stream.called
        assert call.get_waiter('stream_exists') in mock_client.method_calls, "We handled stream existence"

        error_response = {'Error': {'Code': 'Something else'}}
        mock_client.create_stream = Mock(side_effect=ClientError(error_response, 'create_stream'))

        mock_client.reset_mock()
        with pytest.raises(ClientError):
            StreamWriter('blah')
            assert mock_client.create_stream.called
            assert call.get_waiter('stream_exists') not in mock_client.method_calls, "never reached"


def test_put_message(writer):

    writer._send_agg_record = Mock()

    msg = Mock()
    msg.change.xid = 10
    msg.fmt_msg = object()

    writer.last_send = 1445444940.0 - 10      # "2015-10-21 16:28:50"
    with freeze_time('2015-10-21 16:29:00'):  # -> 1445444940.0
        result = writer.put_message(None)

        assert result is None, 'With no message or timeout we did not force a send'
        assert not writer._send_agg_record.called, 'we did not force a send'

        writer._record_agg.add_user_record = Mock(return_value=None)
        result = writer.put_message(msg)
        assert result is None, 'With message, no timeout and not a full agg we do not send'
        assert not writer._send_agg_record.called, 'we did not force a send'

    with freeze_time('2015-10-21 16:29:10'):  # -> 1445444950.0
        result = writer.put_message(None)
        assert result is not None, 'Timeout forced a send'
        assert writer._send_agg_record.called, 'We sent a record'
        assert writer.last_send == 1445444950.0, 'updated window'

    with freeze_time('2015-10-21 16:29:20'):  # -> 1445444960.0
        writer._send_agg_record.reset_mock()
        writer._record_agg.add_user_record = Mock(return_value='blue')
        result = writer.put_message(msg)

        assert result == 'blue', 'We passed in a message that forced the agg to report full'
        assert writer._send_agg_record.called, 'We sent a record'
        assert writer.last_send == 1445444960.0, 'updated window'


def test__send_agg_record(writer):
    assert writer._send_agg_record(None) is None, 'Do not do anything if agg_record is None'

    agg_rec = Mock()
    agg_rec.get_contents = Mock(return_value=(1, 2, 'datablob'))

    err = ClientError({'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'put_record')

    writer._kinesis.put_record = Mock(side_effect=[err, err, err, {'SequenceNumber': 12345}])

    with patch.object(time, 'sleep') as mock_sleep:
        writer._send_agg_record(agg_rec)
        assert mock_sleep.call_count == 3, "We had to back off 3 times so we slept"
        assert mock_sleep.call_args_list == [call(.1), call(.2), call(.4)], 'Geometric back off!'

    with pytest.raises(ClientError):
        writer._kinesis.put_record = Mock(side_effect=ClientError({'Error': {'Code': 'Something else'}},
                                                                  'put_record'))
        writer._send_agg_record(agg_rec)

    writer.back_off_limit = .3  # Will bust on third go around
    writer._kinesis.put_record = Mock(side_effect=[err, err, err, {'SequenceNumber': 12345}])
    with pytest.raises(Exception) as e_info, patch.object(time, 'sleep'):
        writer._send_agg_record(agg_rec)
        assert e_info.value.message == 'ProvisionedThroughputExceededException caused a backed off too many times!', \
            'We raise on too many throughput errors'
