from Prefilters import unwanted_terms_prefilter
from Processors import UserHostIpProcessor, CleanProcessor, UnwantedStartsProcessor, IgnoreQueriesProcessor, SimpleQueryTypeProcessor, RegexReplaceProcessor, RemoveInsertValuesProcessor, ReplaceConstantsProcessor
from Job import Job
from myutils import querytypes

import re

"""
This simple example runs a job that processes the 'test_general_log'
database, storing into the 'test_processed_log' database.
"""

if __name__ == '__main__':

    # Define a Processor to replace long runs of digits with '<numlist
    # len=##>'. This Processor will be passed to the Job when we
    # create it. Other Processors are not created at the same time as
    # the Job
    numlist_sub_processor = RegexReplaceProcessor('query',
                                                  re.compile(r'\([0-9, ]{20,}\)'),
                                                  lambda x: '<numlist len={0}>'
                                                  .format(x.group(0).count(',') + 1))

    # create the Job
    j = Job(selectors = ["command_type in ('Execute', 'Query')"],
            prefilters = [unwanted_terms_prefilter(["information_schema",
                                                    "mysql"],
                                                   flags = re.I),
                      ],
            processors = [numlist_sub_processor,
                          UserHostIpProcessor(users_reject=['buildbot', 'root']),
                          CleanProcessor(),
                          UnwantedStartsProcessor(["SHOW",
                                                   "SET sql_mode",
                                                   "SET NAMES",
                                                   "SET character_set_results"]),
                          IgnoreQueriesProcessor(["SELECT DATABASE()",
                                                  "commit",
                                                  "SET autocommit=0",
                                                  "SET autocommit=1",
                                                  "SELECT @@version_comment LIMIT 1"]),
                          SimpleQueryTypeProcessor(),
                          RemoveInsertValuesProcessor(),
                          ReplaceConstantsProcessor(),
                      ],
            outputs = [('event_time', 'TIMESTAMP'),
                       ('user', 'MEDIUMTEXT'),
                       ('host', 'MEDIUMTEXT'),
                       ('query_type', 'ENUM{0}'.format(querytypes)),
                       ('query', 'MEDIUMTEXT'),
                       ('vals', 'MEDIUMTEXT'),
                   ])
                    
    # run the Job
    j.run('test_processed_log', source_db = 'test_general_log')
