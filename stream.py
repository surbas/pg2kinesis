from time import sleep

import aws_kinesis_agg.aggregator
import boto3
from botocore.exceptions import ClientError
from .log import logger

class StreamWriter(object):
    def __init__(self, stream_name, back_off_limit=60):
        self.stream_name = stream_name
        self.kinesis = boto3.client('kinesis')
        self.back_off_limit = back_off_limit
        self._sequence_number_for_ordering = '0'
        self._record_agg = aws_kinesis_agg.aggregator.RecordAggregator()

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
        agro = None

        if fmt_msg:
            agro = self._record_agg.add_user_record(fmt_msg.change.xid, fmt_msg.fmt_msg)

        # will be and object once the agg is full.
        if agro is not None:
            self._send_agg_record(agro)
            self._record_agg = aws_kinesis_agg.aggregator.RecordAggregator()

        return agro

    def _send_agg_record(self, agg_record):
        if agg_record is None:
            return

        logger.info('Sending %s records' % agg_record.get_num_user_records())
        pk, ehk, data = agg_record.get_contents()
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
                logger.debug('Sequnce number: %s' % result['SequenceNumber'])
                break
