from Processors import UserHostIpProcessor, CleanProcessor
from Job import Job

"""
This simple example runs a job that processes the 'test_general_log'
database, storing into the 'test_processed_log' database.
"""

if __name__ == '__main__':
    j = Job(selectors = ["command_type in ('Execute', 'Query')"],
            prefilters = [],
            processors = [UserHostIpProcessor(users_reject=['buildbot', 'root']),
                          CleanProcessor()],
            outputs = [('event_time', 'TIMESTAMP'),
                       ('user', 'MEDIUMTEXT'),
                       ('host', 'MEDIUMTEXT'),
                       ('query', 'MEDIUMTEXT')])
                                              
    j.run('test_processed_log', source_db = 'test_general_log')
