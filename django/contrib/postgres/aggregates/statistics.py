from django.db import models
from django.db.models.aggregates import Aggregate

__all_ = [
    'CovarPop', 'Corr', 'RegrAvgX', 'RegrAvgY', 'RegrCount',
]


class StatFunc(Aggregate):
    template = "%(function)s(%(y)s, %(x)s)"

    def __init__(self, x, y, output_field=models.FloatField()):
        if not x or not y:
            raise TypeError('X and Y must be explicitly provided. Example: AggrFunc(x="field1", y="field2")')
        self.x = x
        self.y = y
        super(StatFunc, self).__init__(output_field=output_field)

    @property
    def default_alias(self):
        return '%s_%s__%s' % (self.y, self.x, self.name.lower())

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False, for_save=False):
        return super(Aggregate, self).resolve_expression(query, allow_joins, reuse, summarize)

    def as_sql(self, compiler, connection, function=None, template=None):
        template = self.extra.get('template', self.template)
        mapping = {'function': self.function, 'y': self.y, 'x': self.x}
        # mapping, params
        return template % mapping, []


class Corr(StatFunc):
    function = 'CORR'
    name = 'Corr'


class CovarPop(StatFunc):
    name = 'CovarPop'

    def __init__(self, x, y, sample=False):
        self.function = 'COVAR_SAMP' if sample else 'COVAR_POP'
        super(CovarPop, self).__init__(x, y)


class RegrAvgX(StatFunc):
    function = 'REGR_AVGX'
    name = 'RegrAvgX'


class RegrAvgY(StatFunc):
    function = 'REGR_AVGY'
    name = 'RegrAvgY'


class RegrCount(StatFunc):
    function = 'REGR_COUNT'
    name = 'RegrCount'

    def __init__(self, x, y):
        super(RegrCount, self).__init__(x=x, y=y, output_field=models.IntegerField())

    def convert_value(self, value, connection, context):
        if value is None:
            return 0
        return int(value)
