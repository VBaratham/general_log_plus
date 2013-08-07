# This is an example of how to make Prefilters functionally, but
# something this simple should really be done as a selector!
def column_values_prefilter(column, values):
    values = set(values)
    return lambda row: row.get(column) in values
