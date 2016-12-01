import json
import re
import sys

from .log import logger

from backport_collections import namedtuple

# Tuples representing changes as pulled from database
Change = namedtuple('Change', 'xid, table, operation, pkey')

# Final product of Formatter, a Change and the Change formatted.
Message = namedtuple('Message', 'change, fmt_msg')

COL_TYPE_VALUE_TEMPLATE_PAT = r"{col_name}\[{col_type}\]:'?([\w\-]+)'?"

class Formatter(object):
    VERSION = 0
    TYPE = 'CDC'
    IGNORED_CHANGES = {"BEGIN", "COMMIT"}

    def __init__(self, primary_key_map, full_change=False, table_pat=None):
        self.primary_key_patterns = {}
        self.table_re = re.compile(table_pat if table_pat is not None else ur'[/w/._]+')
        self.full_change = full_change
        self.cur_xact = 0

        for k, v in primary_key_map.iteritems():
            # ":" added to make later look up not need to trim trailing ":".
            self.primary_key_patterns[k + ":"] = re.compile(
                COL_TYPE_VALUE_TEMPLATE_PAT.format(col_name=v.col_name, col_type=v.col_type)
            )

    def _preprocess_changes(self, change):
        rec = change.split(' ', 3)

        if rec[0] == "BEGIN":
            self.cur_xact = rec[1]
        elif rec[0] in self.IGNORED_CHANGES:
            pass
        elif rec[0] == 'table':
            table_name = rec[1][:-1]

            if self.table_re.search(table_name):
                try:
                    mat = self.primary_key_patterns[rec[1]].search(rec[3])
                except KeyError:
                    logger.warning('Unable to locate table {0}'.format(rec[1]))
                else:
                    if mat:
                        pkey = mat.groups()[0]
                        return Change(xid=self.cur_xact, table=table_name, operation=rec[2][:-1], pkey=pkey)
        else:
            logger.warning('Unknown change type: %s' % rec)
            raise Exception('Unknown change type: %s' % rec)

    def __call__(self, change):
        pp_change = self._preprocess_changes(change)
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
