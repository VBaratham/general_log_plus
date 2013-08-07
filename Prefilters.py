import re

def unwanted_starts_prefilter(unwanted_starts):
    """
    Return a prefilter that rejects queries that begin with one of the
    elements of @unwanted_starts, which are regexes (or just search
    strings)
    """
    unwanted_starts_re = re.compile('|'.join('{0}.*'.format(x) for x in unwanted_starts))
    return lambda row: unwanted_starts_re.match(row.get('argument') or '')

def unwanted_terms_prefilter(unwanted_terms):
    """
    Return a prefilter that rejects queries that contain one of the
    elemenfs of @unwanted_terms, which are regexes (or just search
    strings)
    """
    unwanted_terms_re = re.compile('|'.join(unwanted_terms))
    return lambda row: unwanted_terms_re.search(row.get('argument') or '')

def ignore_queries_prefilter(queries):
    """
    Return a prefilter that rejects queries that are in @queries
    """
    queries = set(queries)
    return lambda row: (row.get('argument') or '') in queries
