def column_string_values_selector(column, values):
    return "{0} IN ({1})".format(column, ', '.join("'{0}'".format(val) for val in values))

def column_string_value_selector(column, value):
    return "{0} = {1}".format(column, value)
