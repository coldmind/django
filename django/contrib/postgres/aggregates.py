from django.db.models import FloatField
from django.db.models.aggregates import Aggregate

__all_ = [
    'ArrayAgg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr',
    'Corr', 'StringAgg',
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


# Functions for statistics


class StatFunc(Aggregate):
    template = "%(function)s(%(y)s, %(x)s)"

    def __init__(self, x, y):
        if not x or not y:
            raise TypeError('X and Y must be explicitly provided. Example: AggrFunc(x="field1", y="field2")')
        super(StatFunc, self).__init__(output_field=FloatField())
        self.x = x
        self.y = y

    @property
    def default_alias(self):
        return '%s_%s__%s' % (self.y, self.x, self.name)

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False, for_save=False):
        c = super(Aggregate, self).resolve_expression(query, allow_joins, reuse, summarize)
        return c

    def as_sql(self, compiler, connection, function=None, template=None):
        template = self.extra.get('template', self.template)
        params = []
        mapping = {'function': self.function, 'y': self.y, 'x': self.x}
        return template % mapping, params


class Corr(StatFunc):
    function = 'CORR'
    name = 'corr'
