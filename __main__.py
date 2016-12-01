import time
import sys

import click

from .slot import SlotReader
from .formatter import get_formatter
from .stream import StreamWriter
from .log import logger


class Consume(object):
    def __init__(self, formatter, writer, replication_feedback_window=10):
        self.cum_msg_count = 0
        self.cum_msg_size = 0
        self.msg_window_size = 0
        self.msg_window_count = 0
        self.cur_window = 0

        self.formatter = formatter
        self.writer = writer

        self.replication_feedback_window = replication_feedback_window

    def __call__(self, msg):
        self.cum_msg_count += 1
        self.cum_msg_size += msg.data_size

        self.msg_window_size += msg.data_size
        self.msg_window_count += 1

        fmt_msg = self.formatter(msg.payload)
        did_put = self.writer.put_message(fmt_msg)
        if did_put:
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
        elif self.replication_feedback_window:
            int_time = int(time.time())
            if not int_time % self.replication_feedback_window and int_time != self.cur_window:
                logger.info('xid: {:10} mwc:{:>12} mws:{:>12}mb cc{:>16} cs{:>16}mb'.format(
                    formatter.cur_xact, self.msg_window_count, self.msg_window_size / 1048576,
                    self.cum_msg_count, self.cum_msg_size / 1048576))

                msg.cursor.send_feedback(write_lsn=msg.data_start)
                self.cur_window = int_time
                self.msg_window_size = 0
                self.msg_window_count = 0

@click.command()
@click.option('--pg-dbname', '-d', help='Database to connect to.')
@click.option('--pg-host', '-h', default='', help='Postgres server location. Leave empty if localhost.')
@click.option('--pg-port', '-p', default='5432', help='Postgres port.')
@click.option('--pg-user', '-u', help='Postgres user')
@click.option('--pg-slot-name', '-s', default='pg2kinesis', help='Postgres replication slot name.')
@click.option('--stream-name', '-k', default='pg2kinesis', help='Kinesis stream name.')
@click.option('--message-formatter', '-f', default='CSVPayload', help='Kinesis record formatter.')
@click.option('--table-pat', help='Optional regular expression for table names.')
@click.option('--replication-feedback-window', '-r', default=10, is_flag=True, click.IntRange(0, sys.maxint),
              help='How often in seconds to send keep alive feedback. 0 to Disable.')
@click.option('--full-change', default=False, is_flag=True, help='Not yet implemented.')
@click.option('--create-slot', default=False, is_flag=True, help='Attempt to on start create a the slot.')
@click.option('--recreate-slot', default=False, is_flag=True,
              help='Deletes the slot on start if it exists and then creates.')
def main(pg_dbname, pg_host, pg_port, pg_user, pg_slot_name, stream_name, message_formatter, table_pat,
         replication_feedback_window, full_change, create_slot, delete_slot):
    logger.info('Starting pg2kinesis')

    if full_change:
        raise NotImplementedError

    logger.info('Getting kinesis stream writer')
    writer = StreamWriter(stream_name)

    with SlotReader(pg_dbname, pg_host, pg_port, pg_user, pg_slot_name) as reader:

        if delete_slot:
            reader.delete_slot()
            reader.create_slot()

        if create_slot:
            reader.create_slot()

        pk_map = reader.primary_key_map()
        formatter = get_formatter(message_formatter, pk_map, full_change, table_pat)

        consume = Consume(formatter, writer, replication_feedback_window)

        # Blocking. Responds to Control-C.
        reader.process_replication_stream(consume)
