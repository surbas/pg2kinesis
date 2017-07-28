from __future__ import division
import time

import click

from .slot import SlotReader
from .formatter import get_formatter
from .stream import StreamWriter
from .log import logger

@click.command()
@click.option('--pg-dbname', '-d', help='Database to connect to.')
@click.option('--pg-host', '-h', default='', help='Postgres server location. Leave empty if localhost.')
@click.option('--pg-port', '-p', default='5432', help='Postgres port.')
@click.option('--pg-user', '-u', help='Postgres user')
@click.option('--pg-slot-name', '-s', default='pg2kinesis', help='Postgres replication slot name.')
@click.option('--stream-name', '-k', default='pg2kinesis', help='Kinesis stream name.')
@click.option('--message-formatter', '-f', default='CSVPayload', help='Kinesis record formatter.')
@click.option('--table-pat', help='Optional regular expression for table names.')
@click.option('--full-change', default=False, is_flag=True, help='Not yet implemented.')
@click.option('--create-slot', default=False, is_flag=True, help='Attempt to on start create a the slot.')
@click.option('--recreate-slot', default=False, is_flag=True,
              help='Deletes the slot on start if it exists and then creates.')
def main(pg_dbname, pg_host, pg_port, pg_user, pg_slot_name, stream_name,
         message_formatter, table_pat, full_change, create_slot, recreate_slot):
    logger.info('Starting pg2kinesis')

    if full_change:
        raise NotImplementedError

    logger.info('Getting kinesis stream writer')
    writer = StreamWriter(stream_name)

    with SlotReader(pg_dbname, pg_host, pg_port, pg_user, pg_slot_name) as reader:

        if recreate_slot:
            reader.delete_slot()
            reader.create_slot()
        elif create_slot:
            reader.create_slot()

        pk_map = reader.primary_key_map
        formatter = get_formatter(message_formatter, pk_map, full_change, table_pat)

        consume = Consume(formatter, writer)

        # Blocking. Responds to Control-C.
        reader.process_replication_stream(consume)

class Consume(object):
    def __init__(self, formatter, writer):
        self.cum_msg_count = 0
        self.cum_msg_size = 0
        self.msg_window_size = 0
        self.msg_window_count = 0
        self.cur_window = 0

        self.formatter = formatter
        self.writer = writer

    def __call__(self, change):
        self.cum_msg_count += 1
        self.cum_msg_size += change.data_size

        self.msg_window_size += change.data_size
        self.msg_window_count += 1

        fmt_msg = self.formatter(change.payload)
        did_put = self.writer.put_message(fmt_msg)
        if did_put:
            change.cursor.send_feedback(flush_lsn=change.data_start)
            logger.info('Flushed LSN: {}'.format(change.data_start))

        int_time = int(time.time())
        if not int_time % 10 and int_time != self.cur_window:
            logger.info('xid: {:12} win_count:{:>10} win_size:{:>10}mb cum_count:{:>10} cum_size:{:>10}mb'.format(
                self.formatter.cur_xact, self.msg_window_count, self.msg_window_size / 1048576,
                self.cum_msg_count, self.cum_msg_size / 1048576))

            self.cur_window = int_time
            self.msg_window_size = 0
            self.msg_window_count = 0

if __name__ == '__main__':
    main()
