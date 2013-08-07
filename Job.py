import os
import MySQLdb as mysql
from MySQLdb.cursors import DictCursor

from operator import itemgetter

from Exceptions import InvalidSortException, SkipRowException

class Job:

    def __init__(self, selectors = [], prefilters = [], processors = [],
                 outputs = {}):
        """
        selectors  - list of sql statements to be put in the WHERE clause
                     of the query that selects data from the general_log

        prefilters - list of Prefilter objects to use in this Job

        processors - list of Processor objects to use in this Job

        outputs    - list of tuples of (output column, type) where types
                     are specified as sql types (such as in a table
                     schema definition)
        """
        self.selectors = selectors
        self.prefilters = prefilters
        self.processors = processors
        self.outputs = outputs

        self._input_output_validate()
        
    def _sort_schema(self):
        """
        Returns the list of Processors broken into sublists separated by
        Processors that require sorting, such that the first Processor in
        each sublist (except the first) requires sorting. Second return
        value is the sql sorting to use when selecting general_log data.
        Raises InvalidSortException if the sort order is invalid (sql sort
        after another sort, or two sql sorts)
        """

        found_sort = False
        sql_sort = ''
        allprocs = [[]]

        for processor in self.processors:
            if processor.sort_column:
                found_sort = True
                allprocs.append([processor])
            elif processor.sql_sort_by:
                if found_sort:
                    raise InvalidSortException
                found_sort = True
                sql_sort = processor.sql_sort_by
                allprocs[-1].append(processor)
            else:
                allprocs[-1].append(processor)

        return filter(lambda x: x != [], allprocs), sql_sort

    def _input_output_validate(self):
        """
        Return True if the input/outputs of self.processors are consistent,
        that is, each input is an output from a previous processor or from
        the general_log itself. Note: Processors specify inputs, but there
        is nothing stopping them from attempting to access other columns,
        which may or maynot exist.

        # TODO: Should there be some safeguard against this?
        """
        cols = set(['event_time', 'user_host', 'thread_id', 'server_id',
                    'command_type', 'argument'])
        for proc in self.processors:
            for col in proc.inputs:
                if col not in cols:
                    return False
            cols.update(proc.inputs)


    def _all_prefilters_pass(self, row):
        """
        Return True if all of self.prefilters accept @row
        """
        for fil in self.prefilters:
            if not fil(row):
                return False
        return True

    def _tables_to_reduce(self, target_db, source_db):
        """
        Return the tables in @source_db but not in @target_db
        """
        conn = mysql.connect(**{"host": "localhost",
                                "user": "root",
                                "passwd": "",
                                "unix_socket": "/u1/vbar/mysql/thesock"})
        cur = conn.cursor()

        cur.execute("USE {0}".format(source_db))
        cur.execute("SHOW TABLES")
        gen_log_tables = set([x for x, in cur.fetchall()])

        cur.execute("USE {0}".format(target_db))
        cur.execute("SHOW TABLES")
        target_db_tables = set([x for x, in cur.fetchall()])

        tables = gen_log_tables - target_db_tables

        cur.close()
        conn.close()

        return tables
            

    def run(self, target_db, source_db = 'general_log'):

        sort_schema, sql_sort_by = self._sort_schema()
        sql_where = ( "WHERE " + " AND ".join('(' + sel + ')'
                                              for sel in self.selectors) ) \
                                                  if self.selectors else ''

        final_selector = itemgetter(*[colname for colname, coltype
                                      in self.outputs])

        conn = mysql.connect(**{"host": "localhost",
                                "user": "root",
                                "passwd": "",
                                "unix_socket": "/u1/vbar/mysql/thesock",
                                "cursorclass": DictCursor})
        cur = conn.cursor()
            
        for table in self._tables_to_reduce(target_db, source_db):
            cur.execute("USE {0}".format(source_db))
            sql_full = "SELECT * FROM {0} {1} {2}".format(table,
                                                          sql_where,
                                                          sql_sort_by)

            cur.execute(sql_full)

            rows = filter(self._all_prefilters_pass, cur.fetchall())

            # TODO: can avoid using DictCursor by changing the above line

            for group in sort_schema:
                newrows = []
                if group[0].sort_column:
                    rows.sort(key=itemgetter(group[0].sort_column),
                              reverse=group[0].sort_reverse)
                for row in rows:
                    try:
                        for processor in group:
                            row.update(zip(processor.outputs,
                                           processor.process(row)))
                        newrows.append(row)
                    except SkipRowException:
                        continue
                rows = newrows
            
            temp_filename = '{0}.tmp'.format(table)
            with open(temp_filename, 'w') as outfile:
                print >>outfile, '\n'.join('\t'.join(str(x) for x in
                                                     final_selector(row))
                                           for row in rows)

            cur.execute("USE {0}".format(target_db))
            cur.execute("CREATE TABLE {0} (".format(table) + \
                        ',\n'.join("{0} {1}".format(col, typ)
                                   for col, typ in self.outputs) + \
                        ")")
            cur.execute("LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}"
                        .format(temp_filename, table))
            
            os.remove(temp_filename)

        conn.commit()
        cur.close()
        conn.close()
