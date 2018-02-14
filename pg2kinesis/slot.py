from collections import namedtuple
import threading

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import psycopg2.errorcodes

from .log import logger

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, None)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, None)

PrimaryKeyMapItem = namedtuple('PrimaryKeyMapItem', 'table_name, col_name, col_type, col_ord_pos')


class SlotReader(object):
    PK_SQL = """
    SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, ordinal_position
    FROM information_schema.tables
    LEFT JOIN (
        SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, c.ordinal_position,
                    table_catalog, table_schema, table_name
        FROM information_schema.table_constraints
        JOIN information_schema.key_column_usage AS kcu
            USING (constraint_catalog, constraint_schema, constraint_name,
                        table_catalog, table_schema, table_name)
        JOIN information_schema.columns AS c
            USING (table_catalog, table_schema, table_name, column_name)
        WHERE constraint_type = 'PRIMARY KEY'
    ) as q using (table_catalog, table_schema, table_name)
    ORDER BY ordinal_position;
    """

    def __init__(self, database, host, port, user, slot_name,
                 output_plugin='test_decoding', keepalive_window=30):
        # Cool fact: using connections as context manager doesn't close them on
        # success after leaving with block
        self._db_confg = dict(database=database, host=host, port=port, user=user)
        self._keepalive_window = keepalive_window
        self._repl_conn = None
        self._repl_cursor = None
        self._normal_conn = None
        self.slot_name = slot_name
        self.output_plugin = output_plugin
        self.cur_lag = 0

    def __enter__(self):
        self._normal_conn = self._get_connection()
        self._normal_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._repl_conn = self._get_connection(connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self._repl_cursor = self._repl_conn.cursor()

        if self._keepalive_window:
            self._keepalive_thread = self._send_keepalive()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Be a good citezen and try to clean up on the way out.
        """
        if self._keepalive_thread:
            try:
                self._keepalive_thread.join(timeout=self._keepalive_window+3)
            except Exception:
                pass

        try:
            self._repl_cursor.close()
        except Exception:
            pass

        try:
            self._repl_conn.close()
        except Exception:
            pass

        try:
            self._normal_conn.close()
        except Exception:
            pass


    def _send_keepalive(self):

        # keepalive with no args.
        try:
            self._repl_cursor.send_feedback()
        except psycopg2.DatabaseError as e:
            if not e.message == 'no COPY in progress\n':
                logger.exception(e)
        except Exception as e:
            logger.exception(e)

        # schedule myself in the future
        t = threading.Timer(self._keepalive_window, self._send_keepalive)
        t.daemon = True
        t.start()
        return t

    def _get_connection(self, connection_factory=None, cursor_factory=None):
        return psycopg2.connect(connection_factory=connection_factory,
                                cursor_factory=cursor_factory, **self._db_confg)

    def _execute_and_fetch(self, sql, *params):
        with self._normal_conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            return cur.fetchall()

    @property
    def primary_key_map(self):
        logger.info('Getting primary key map')
        result = map(PrimaryKeyMapItem._make, self._execute_and_fetch(SlotReader.PK_SQL))
        pk_map = {rec.table_name: rec for rec in result}

        return pk_map

    def create_slot(self):
        logger.info('Creating slot %s' % self.slot_name)
        try:
            self._repl_cursor.create_replication_slot(self.slot_name,
                                                      slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                      output_plugin=self.output_plugin)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info('Slot %s is already present.' % self.slot_name)

    def delete_slot(self):
        logger.info('Deleting slot %s' % self.slot_name)
        try:
            self._repl_cursor.drop_replication_slot(self.slot_name)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.UNDEFINED_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info('Slot %s was not found.' % self.slot_name)

    def process_replication_stream(self, consume):
        logger.info('Starting the consumption of slot "%s"!' % self.slot_name)
        if self.output_plugin == 'wal2json':
            options = {'include-xids': 1}
        else:
            options = None
        self._repl_cursor.start_replication(self.slot_name, options=options)
        self._repl_cursor.consume_stream(consume)
