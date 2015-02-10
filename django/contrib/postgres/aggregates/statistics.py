from django.db.models import F, FloatField, IntegerField
from django.db.models.aggregates import Aggregate
from django.db.models.expressions import Value
from django.utils import six

__all_ = [
    'CovarPop', 'Corr', 'RegrAvgX', 'RegrAvgY', 'RegrCount', 'RegrIntercept',
    'RegrR2', 'RegrSlope', 'RegrSXX', 'RegrSXY',
]


class StatFunc(Aggregate):
    template = "%(function)s(%(y)s, %(x)s)"
    _num_expression_alias = 'num'

    def __init__(self, y, x, output_field=FloatField()):
        if not x or not y:
            raise TypeError('Both X and Y must be provided.')
        if (not isinstance(x, (six.text_type, six.string_types)) or
                not isinstance(y, (six.text_type, six.string_types))):
            raise ValueError('X and Y must be a string.')
        super(StatFunc, self).__init__(output_field=output_field)
        self.x = x
        self.y = y
        self.source_expressions = self.parse_expressions([self.x, self.y])

    def parse_expressions(self, expressions):
        # Some stat functions allows integer to be an argument,
        # so we need to parse it and not resolve expression as F for
        # this case.
        return [
            Value(arg) if arg.isdigit() else F(arg) for arg in expressions
        ]

    @property
    def default_alias(self):
        # Since number is allowed to be an expression,
        # we need to have kinda "static" alias for this case.
        x = self._num_expression_alias if self.x.isdigit() else self.x
        y = self._num_expression_alias if self.y.isdigit() else self.y
        return '%s_%s__%s' % (y, x, self.name.lower())

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False, for_save=False):
        return super(Aggregate, self).resolve_expression(query, allow_joins, reuse, summarize)

    def as_sql(self, compiler, connection, function=None, template=None):
        sql_parts = []
        params = []
        for arg in self.source_expressions:
            arg_sql, arg_params = compiler.compile(arg)
            sql_parts.append(arg_sql)
            params.extend(arg_params)
        mapping = {'function': self.function, 'y': sql_parts[1], 'x': sql_parts[0]}
        return self.template % mapping, params


class Corr(StatFunc):
    function = 'CORR'
    name = 'Corr'


class CovarPop(StatFunc):
    name = 'CovarPop'

    def __init__(self, y, x, sample=False):
        self.function = 'COVAR_SAMP' if sample else 'COVAR_POP'
        super(CovarPop, self).__init__(y, x)


class RegrAvgX(StatFunc):
    function = 'REGR_AVGX'
    name = 'RegrAvgX'


class RegrAvgY(StatFunc):
    function = 'REGR_AVGY'
    name = 'RegrAvgY'


class RegrCount(StatFunc):
    function = 'REGR_COUNT'
    name = 'RegrCount'

    def __init__(self, y, x):
        super(RegrCount, self).__init__(y=y, x=x, output_field=IntegerField())

    def convert_value(self, value, connection, context):
        if value is None:
            return 0
        return int(value)


class RegrIntercept(StatFunc):
    function = 'REGR_INTERCEPT'
    name = 'RegrIntercept'


class RegrR2(StatFunc):
    function = 'REGR_R2'
    name = 'RegrR2'


class RegrSlope(StatFunc):
    function = 'REGR_SLOPE'
    name = 'RegrSlope'


class RegrSXX(StatFunc):
    function = 'REGR_SXX'
    name = 'RegrSXX'


class RegrSXY(StatFunc):
    function = 'REGR_SXY'
    name = 'RegrSXY'
