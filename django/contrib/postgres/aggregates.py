from django.db.models.aggregates import Aggregate

__all_ = [
    'ArrayAgg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr',
    'StringAgg',
]

# General-purpose functions


class ArrayAgg(Aggregate):
    function = 'ARRAY_AGG'
    name = 'ArrayAgg'

    def convert_value(self, value, connection, context):
        if not value:
            return []
        return value


class BitAnd(Aggregate):
    function = 'BIT_AND'
    name = 'BitAnd'


class BitOr(Aggregate):
    function = 'BIT_OR'
    name = 'BitOr'


class BoolAnd(Aggregate):
    function = 'BOOL_AND'
    name = 'BoolAnd'


class BoolOr(Aggregate):
    function = 'BOOL_OR'
    name = 'BoolOr'


class StringAgg(Aggregate):
    function = 'STRING_AGG'
    name = 'StringAgg'
    template = "%(function)s(%(expressions)s, '%(delimiter)s')"

    def __init__(self, *expressions, **extra):
        delimiter = extra.get('delimiter')
        if not delimiter:
            raise TypeError('STRING_AGG function requires a delimiter.')
        super(StringAgg, self).__init__(*expressions, **extra)
