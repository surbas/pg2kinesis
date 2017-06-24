import json
import re
import sys

from .log import logger

from collections import namedtuple

# Tuples representing changes as pulled from database
Change = namedtuple('Change', 'xid, table, operation, pkey')

# Final product of Formatter, a Change and the Change formatted.
Message = namedtuple('Message', 'change, fmt_msg')

COL_TYPE_VALUE_TEMPLATE_PAT = ur"{col_name}\[{col_type}\]:'?([\w\-]+)'?"

class Formatter(object):
    VERSION = 0
    TYPE = 'CDC'
    IGNORED_CHANGES = {'COMMIT'}

    def __init__(self, primary_key_map, full_change=False, table_pat=None):
        self._primary_key_patterns = {}
        self.table_re = re.compile(table_pat if table_pat is not None else ur'[/w/._]+')
        self.full_change = full_change
        self.cur_xact = ''

        for k, v in primary_key_map.iteritems():
            # ":" added to make later look up not need to trim trailing ":".
            self._primary_key_patterns[k + ":"] = re.compile(
                COL_TYPE_VALUE_TEMPLATE_PAT.format(col_name=v.col_name, col_type=v.col_type)
            )

    def _preprocess_change(self, change):
        """
        Takes a message payload and distills it into a Change tuple currently only looking for primary key.

        They look like this:
            u"table table_test: UPDATE: uuid[uuid]:'00079f3e-0479-4475-acff-4f225cc5188a' another_col[text]'bling'"

        :param change: a message payload from postgres' test_decoding plugin.
        :return: A namedtuple of type Change
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
                    self._log_and_raise('Unable to locate table: "{}"'.format(rec[1]))
                else:
                    if mat:
                        pkey = mat.groups()[0]
                        return Change(xid=self.cur_xact, table=table_name, operation=rec[2][:-1], pkey=pkey)
                    else:
                        # TODO: make this an error or warning.
                        # self._log_and_raise('Unable to locate primary key for table "{}"'.format(table_name))
                        pass
        else:
            self._log_and_raise('Unknown change: "{}"'.format(change))

    @staticmethod
    def _log_and_raise(msg):
        logger.error(msg)
        raise Exception(msg)

    def __call__(self, change):
        pp_change = self._preprocess_change(change)
        if pp_change:
            return self.produce_formatted_message(pp_change)

    def produce_formatted_message(self, change):
        return change


class CSVFormatter(Formatter):
    def produce_formatted_message(self, change):
        msg = Message(change=change, fmt_msg='{},{},{},{},{},{}'.format(CSVFormatter.VERSION,
                                                                         CSVFormatter.TYPE, *change))
        return msg


class CSVPayloadFormatter(Formatter):
    def produce_formatted_message(self, change):
        msg = Message(change=change, fmt_msg='{},{},{}'.format(CSVFormatter.VERSION,
                                                                CSVFormatter.TYPE, json.dumps(change.__dict__)))
        return msg


def get_formatter(name, primary_key_map, primary_keys_only, table_pat):
    current_module = sys.modules[__name__]
    return getattr(current_module, '%sFormatter' % name)(primary_key_map, primary_keys_only, table_pat)
