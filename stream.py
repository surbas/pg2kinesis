from time import sleep, time

import aws_kinesis_agg.aggregator
import boto3

from botocore.exceptions import ClientError
from .log import logger

class StreamWriter(object):
    def __init__(self, stream_name, back_off_limit=60, send_window=13):
        self.stream_name = stream_name
        self.kinesis = boto3.client('kinesis')
        self.back_off_limit = back_off_limit
        self._sequence_number_for_ordering = '0'
        self._record_agg = aws_kinesis_agg.aggregator.RecordAggregator()
        self._send_window = send_window

        self.last_send = 0

        try:
            self.kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        except ClientError as e:
            # ResourceInUseException is raised when the stream already exists
            if e.response["Error"]["Code"] != "ResourceInUseException":
                logger.error(e)
                raise

        waiter = self.kinesis.get_waiter('stream_exists')

        # waits up to 180 seconds for stream to exist
        waiter.wait(StreamName=self.stream_name)

    def put_message(self, fmt_msg):
        agg_record = None

        if fmt_msg:
            agg_record = self._record_agg.add_user_record(fmt_msg.change.xid, fmt_msg.fmt_msg)

        # agg_record will be a complete record if aggregation is full.
        if agg_record or (self._send_window and time() - self.last_send > self._send_window):
            agg_record = agg_record if agg_record else self._record_agg.clear_and_get()
            self._send_agg_record(agg_record)
            self.last_send = time()

        return agg_record

    def _send_agg_record(self, agg_record):
        if agg_record is None:
            return

        pk, ehk, data = agg_record.get_contents()
        logger.info('Sending %s records. Size %s. PK: %s' %
                    (agg_record.get_num_user_records(), agg_record.get_size_bytes(), pk))

        back_off = .05
        while True:
            try:
                result = self.kinesis.put_record(Data=data,
                                                 ExplicitHashKey=ehk,
                                                 PartitionKey=pk,
                                                 SequenceNumberForOrdering=self._sequence_number_for_ordering,
                                                 StreamName=self.stream_name)

            except ClientError as e:
                if e.response['Error']["Code"] == 'ProvisionedThroughputExceededException':
                    back_off *= 2
                    logger.warning('Provisioned throughput exceeded: sleeping %ss' % back_off)
                    sleep(back_off)
                else:
                    logger.error(e)
                    raise
            else:
                logger.debug('Sequence number: %s' % result['SequenceNumber'])
                break
