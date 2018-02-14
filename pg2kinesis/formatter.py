import json
import re
import sys

from .log import logger

from collections import namedtuple

# Tuples representing changes as pulled from database
Change = namedtuple('Change', 'xid, table, operation, pkey')
FullChange = namedtuple('FullChange', 'xid, change')

# Final product of Formatter, a Change and the Change formatted.
Message = namedtuple('Message', 'change, fmt_msg')

COL_TYPE_VALUE_TEMPLATE_PAT = ur"{col_name}\[{col_type}\]:'?([\w\-]+)'?"
MISSING_TABLE_ERR = 'Unable to locate table: "{}"'
MISSING_PK_ERR = 'Unable to locate primary key for table "{}"'

class Formatter(object):
    VERSION = 0
    TYPE = 'CDC'
    IGNORED_CHANGES = {'COMMIT'}

    def __init__(self, primary_key_map, output_plugin='test_decoding',
                 full_change=False, table_pat=None):

        self._primary_key_patterns = {}
        self.output_plugin = output_plugin
        self.primary_key_map = primary_key_map
        self.full_change = full_change
        self.table_pat = table_pat if table_pat is not None else ur'[\w_\.]+'
        self.table_re = re.compile(self.table_pat)
        self.cur_xact = ''

        for k, v in primary_key_map.iteritems():
            # ":" added to make later look up not need to trim trailing ":".
            self._primary_key_patterns[k + ":"] = re.compile(
                COL_TYPE_VALUE_TEMPLATE_PAT.format(col_name=v.col_name, col_type=v.col_type)
            )

    def _preprocess_test_decoding_change(self, change):
        """
        Takes a message payload from the test_decoding plugin and distills it
        into a Change tuple currently only looking for primary key.

        They look like this:
            u"table table_test: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a' another_col[text]'bling'"

        :param change: a message payload from postgres' test_decoding plugin.
        :return: A list of type Change
        """

        rec = change.split(' ', 3)

        if rec[0] == 'BEGIN':
            self.cur_xact = rec[1]
        elif rec[0] in self.IGNORED_CHANGES:
            pass
        elif rec[0] == 'table':
            table_name = rec[1][:-1]

            if self.table_re.search(table_name):
                try:
                    mat = self._primary_key_patterns[rec[1]].search(rec[3])
                except KeyError:
                    self._log_and_raise(MISSING_TABLE_ERR.format(rec[1]))
                else:
                    if mat:
                        pkey = mat.groups()[0]
                        return [Change(xid=self.cur_xact, table=table_name,
                                       operation=rec[2][:-1], pkey=pkey)]
                    else:
                        self._log_and_raise(MISSING_PK_ERR.format(table_name))
        else:
            self._log_and_raise('Unknown change: "{}"'.format(change))

        return []

    def _preprocess_wal2json_change(self, change):
        """
        Takes a message payload from the wal2json plugin and distills it into a
        list of Change or FullChange tuples.

        They look like this:
            {
                "xid": 1234567890
                "change": [
                    {
                        "kind": "insert",
                        "schema": "public",
                        "table": "some_table",
                        "columnnames": ["id"],
                        "columntypes": ["int4"],
                        "columnvalues": [42]
                    }
                ]
            }
        :param change: a message payload from postgres wal2json plugin.
        :return: A list of type Change or FullChange
        """

        change_dictionary = json.loads(change)
        if not change_dictionary:
            return None

        self.cur_xact = change_dictionary['xid']
        changes = []

        for change in change_dictionary['change']:
            table_name = change['table']
            schema = change['schema']
            if self.table_re.search(table_name):
                if self.full_change:
                    changes.append(FullChange(xid=self.cur_xact, change=change))
                else:
                    try:
                        full_table = '{}.{}'.format(schema, table_name)
                        primary_key = self.primary_key_map[full_table]
                    except KeyError:
                        self._log_and_raise(MISSING_TABLE_ERR.format(full_table))
                    else:
                        value_index = change['columnnames'].index(primary_key.col_name)
                        pkey = str(change['columnvalues'][value_index])
                        changes.append(Change(xid=self.cur_xact,
                                              table=full_table,
                                              operation=change['kind'].lower(),
                                              pkey=pkey))
        return changes

    @staticmethod
    def _log_and_raise(msg):
        logger.error(msg)
        raise Exception(msg)

    def __call__(self, change):
        if self.output_plugin == 'test_decoding':
            pp_changes = self._preprocess_test_decoding_change(change)
        elif self.output_plugin == 'wal2json':
            pp_changes = self._preprocess_wal2json_change(change)
        return [self.produce_formatted_message(pp_change) for pp_change in pp_changes]

    def produce_formatted_message(self, change):
        return change


class CSVFormatter(Formatter):
    VERSION = 0
    def produce_formatted_message(self, change):
        fmt_msg = '{},{},{},{},{},{}'.format(CSVFormatter.VERSION,
                                             CSVFormatter.TYPE, *change)
        return Message(change=change, fmt_msg=fmt_msg)


class CSVPayloadFormatter(Formatter):
    VERSION = 0
    def produce_formatted_message(self, change):
        fmt_msg = '{},{},{}'.format(CSVFormatter.VERSION, CSVFormatter.TYPE,
                                    json.dumps(change.__dict__))
        return Message(change=change, fmt_msg=fmt_msg)


def get_formatter(name, primary_key_map, output_plugin, full_change, table_pat):
    formatter_f = getattr(sys.modules[__name__], '%sFormatter' % name)
    return formatter_f(primary_key_map, output_plugin, full_change, table_pat)
