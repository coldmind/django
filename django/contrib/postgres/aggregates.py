from django.db.models.aggregates import Aggregate

__all_ = [
    'ArrayAgg', 'BitAnd', 'BitOr', 'BoolAnd',
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
