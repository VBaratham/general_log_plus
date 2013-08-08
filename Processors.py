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
    Create 'user', 'host', and 'ip' columns by parsing the 'user_host'
    column. Reject rows with certain users, hosts, or ip's.
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

class UnwantedTermsProcessor(BaseProcessor):
    """
    Reject queries that contain one of several unwanted terms
    """

    def __init__(self, unwanted_terms, flags=0):
        super(UnwantedTermsProcessor, self).__init__()
        self.unwanted_terms_re = re.compile('|'.join(unwanted_terms), flags=flags)
        
        self.inputs = ['query']
        self.outputs = []

    def process(self, row):
        if self.unwanted_terms_re.search(row.get('query')):
            raise SkipRowException
        return tuple()

class UnwantedStartsProcessor(BaseProcessor):
    """
    Reject queries that start with one of several unwanted starts
    """

    def __init__(self, unwanted_starts, flags=0):
        super(UnwantedStartsProcessor, self).__init__()
        self.unwanted_starts_re = re.compile('|'.join('{0}.*'.format(x) for x in unwanted_starts),
                                             flags=flags)
        
        self.inputs = ['query']
        self.outputs = []

    def process(self, row):
        if self.unwanted_starts_re.match(row.get('query')):
            raise SkipRowException
        return tuple()

class IgnoreQueriesProcessor(BaseProcessor):
    """
    Reject some specific queries
    """

    def __init__(self, ignore_queries):
        super(IgnoreQueriesProcessor, self).__init__()
        self.ignore_queries = set(ignore_queries)

        self.inputs = ['query']
        self.outputs = []

    def process(self, row):
        if row.get('query') in self.ignore_queries:
            raise SkipRowException
        return tuple()

class SimpleQueryTypeProcessor(BaseProcessor):
    """
    Add a column of the type of each query
    """

    def __init__(self):
        super(SimpleQueryTypeProcessor, self).__init__()
        self.inputs = ['query']
        self.outputs = ['query_type']

    def process(self, row):
        query = row.get('query')
        if query.startswith('INSERT INTO'):
            return "INSERT",
        if query.startswith("SELECT"):
            return "SELECT",
        if query.startswith("CREATE TABLE"):
            return "CREATE_TABLE",
        if query.startswith("SET"):
            return "SET",
        if query.startswith("LOAD DATA"):
            return "LOAD",
        if query.startswith("ALTER"):
            return "ALTER",
        return "OTHER",

class RegexReplaceProcessor(BaseProcessor):
    """
    Perform regex replacement on a certain column
    """

    def __init__(self, field, regex, replace_fcn):
        super(RegexReplaceProcessor, self).__init__()
        
        self.field = field
        self.regex = regex
        self.sub_fcn = replace_fcn
        
        self.inputs = [field]
        self.outputs = [field]

    def process(self, row):
        return self.regex.sub(self.sub_fcn, row.get(self.field)),

class TypeRegexReplaceProcessor(RegexReplaceProcessor):
    """
    Run a regex replace, like RegexReplaceProcessor, but only on
    certain query types
    """

    def __init__(self, query_types, field, regex, replace_fcn):
        super(TypeRegexReplaceProcessor, self).__init__(field, regex, replace_fcn)
        self.query_types = query_types

        self.inputs.append('query_type')

    def process(self, row):
        if row.get('query_type') in self.query_types:
            return super(TypeRegexReplaceProcessor, self).process(row)
        return row.get(self.field)

class RemoveInsertValuesProcessor(BaseProcessor):
    """
    Remove the values from INSERT statements, replace with '<values>'
    """

    def __init__(self):
        super(RemoveInsertValuesProcessor, self).__init__()
        
        self.inputs = ['query_type', 'query']
        self.outputs = ['query']

        # TODO: make this regex accept dots in table names
        self.insert_re = re.compile(r"(INSERT INTO ['`]?\w+['`]?)")
        self.values_re = re.compile(r'VALUES', re.I)

    def process(self, row):
        if row.get('query_type') == 'INSERT' and self.values_re.search(row.get('query')):
            return (self.insert_re.match(row.get('query')).group(0) + ' <values>'),
        return row.get('query'),

class ReplaceConstantsProcessor(BaseProcessor):
    """
    Replace numerical constants with @replchar and concatenates the
    values, separated by @sepchar, into the 'vals' column.
    """

    def __init__(self, replchar = '?', sepchar = ' ~ '):
        super(ReplaceConstantsProcessor, self).__init__()

        self.replchar = replchar
        self.sepchar = sepchar

        self.const_re = re.compile(r'[<>=]+ *([-0-9.e]+)')

        self.inputs = ['query']
        self.outputs = ['query', 'vals']

    def process(self, row):
        vals = []
        query = row.get('query')
        for m in self.const_re.finditer(query):
            vals.append(m.group(1))
            query = query.replace(m.group(),
                                  m.group().replace(m.group(1),
                                                    self.replchar))
            
        return query, self.sepchar.join(vals)
