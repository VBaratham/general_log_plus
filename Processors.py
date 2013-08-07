import sys
import re

from Exceptions import SkipRowException

from myutils import clean, get_reserved_words

class BaseProcessor(object):
    def __init__(self):
        self.sort_column = None
        self.sql_sort_by = None

class UserHostIpProcessor(BaseProcessor):
    """
    Create 'user', 'host', and 'ip' columns by parsing the
    'user_host' column.
    """

    def __init__(self, users_reject = [], hosts_reject = [], ip_reject = []):
        super(UserHostIpProcessor, self).__init__()
        self.users_reject = set(users_reject)
        self.hosts_reject = set(hosts_reject)
        self.ip_reject = set(ip_reject)
        
        self.inputs = ['user_host']
        self.outputs = ['user', 'host', 'ip']

        self.user_host_re = re.compile(r".*\[(?P<uname>.*)\] @ (?P<host>.*) \[(?P<ip>.*)\]")

    def process(self, row):
        m = self.user_host_re.match(row.get('user_host') or '')
        if not m:
            print >>sys.stderr, "Could not parse user_host: %s" % row.get('user_host')
        
        user, host, ip = m.group('uname', 'host', 'ip')
        if user in self.users_reject or host in self.hosts_reject or ip in self.ip_reject:
            raise SkipRowException

        return user, host, ip

class CleanProcessor(BaseProcessor):
    """
    Capitalize all sql keywords and replace all runs of whitespace
    with a single whitespace character.
    """

    def __init__(self):
        super(CleanProcessor, self).__init__()
        self.inputs = ['argument']
        self.outputs = ['query']
        self.reserved_words = get_reserved_words('mysql_keywords.txt')

    def process(self, row):
        return clean(row.get('argument') or '', reserved_words = self.reserved_words),

