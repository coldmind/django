import unittest

from django.contrib.postgres.aggregates import (
    ArrayAgg, BitAnd, BitOr, BoolAnd, BoolOr, CovarPop, Corr,
    RegrAvgX, RegrAvgY, RegrCount, StringAgg,
)
from django.db import connection
from django.test import TestCase
from django.test.utils import Approximate

from .models import GeneralTestModel, StatTestModel


@unittest.skipUnless(connection.vendor == 'postgresql', 'PostgreSQL required')
class TestGeneralAggregate(TestCase):
    fixtures = ["aggregation_general.json"]

    def test_array_agg_charfield(self):
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': ['Foo1', 'Foo2', 'Foo3', 'Foo4']})

    def test_array_agg_integerfield(self):
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': [0, 1, 2, 0]})

    def test_array_agg_booleanfield(self):
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': [True, False, False, True]})

    def test_array_agg_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': []})
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': []})
        values = GeneralTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': []})

    def test_bit_and_general(self):
        values = GeneralTestModel.objects.filter(
            integer_field__in=[0, 1]).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 0})

    def test_bit_and_on_only_true_values(self):
        values = GeneralTestModel.objects.filter(
            integer_field=1).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 1})

    def test_bit_and_on_only_false_values(self):
        values = GeneralTestModel.objects.filter(
            integer_field=0).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 0})

    def test_bit_and_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': None})

    def test_bit_or_general(self):
        values = GeneralTestModel.objects.filter(
            integer_field__in=[0, 1]).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 1})

    def test_bit_or_on_only_true_values(self):
        values = GeneralTestModel.objects.filter(
            integer_field=1).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 1})

    def test_bit_or_on_only_false_values(self):
        values = GeneralTestModel.objects.filter(
            integer_field=0).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 0})

    def test_bit_or_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': None})

    def test_bool_and_general(self):
        values = GeneralTestModel.objects.all().aggregate(BoolAnd('boolean_field'))
        self.assertEqual(values, {'boolean_field__booland': False})

    def test_bool_and_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(BoolAnd('boolean_field'))
        self.assertEqual(values, {'boolean_field__booland': None})

    def test_bool_or_general(self):
        values = GeneralTestModel.objects.all().aggregate(BoolOr('boolean_field'))
        self.assertEqual(values, {'boolean_field__boolor': True})

    def test_bool_or_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(BoolOr('boolean_field'))
        self.assertEqual(values, {'boolean_field__boolor': None})

    def test_string_agg_requires_delimiter(self):
        with self.assertRaises(TypeError):
            GeneralTestModel.objects.all().aggregate(StringAgg('char_field'))

    def test_string_agg_charfield(self):
        values = GeneralTestModel.objects.all().aggregate(StringAgg('char_field', delimiter=';'))
        self.assertEqual(values, {'char_field__stringagg': 'Foo1;Foo2;Foo3;Foo4'})

    def test_string_agg_empty_result(self):
        GeneralTestModel.objects.all().delete()
        values = GeneralTestModel.objects.all().aggregate(StringAgg('char_field', delimiter=';'))
        self.assertEqual(values, {'char_field__stringagg': None})


@unittest.skipUnless(connection.vendor == 'postgresql', 'PostgreSQL required')
class TestStatisticsAggregate(TestCase):
    fixtures = ["aggregation_statistics.json"]

    def test_corr_general(self):
        values = StatTestModel.objects.all().aggregate(Corr(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__corr': -1.0})

    def test_corr_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(Corr(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__corr': None})

    def test_covar_pop_general(self):
        values = StatTestModel.objects.all().aggregate(CovarPop(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__covarpop': Approximate(-0.66, places=1)})

    def test_covar_pop_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(CovarPop(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__covarpop': None})

    def test_covar_pop_sample(self):
        values = StatTestModel.objects.all().aggregate(CovarPop(y='int2', x='int1', sample=True))
        self.assertEqual(values, {'int2_int1__covarpop': -1.0})

    def test_covar_pop_sample_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(CovarPop(y='int2', x='int1', sample=True))
        self.assertEqual(values, {'int2_int1__covarpop': None})

    def test_regr_avgx_general(self):
        values = StatTestModel.objects.all().aggregate(RegrAvgX(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regravgx': 2.0})

    def test_regr_avgx_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(RegrAvgX(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regravgx': None})

    def test_regr_avgy_general(self):
        values = StatTestModel.objects.all().aggregate(RegrAvgY(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regravgy': 2.0})

    def test_regr_avgy_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(RegrAvgY(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regravgy': None})

    def test_regr_count_general(self):
        values = StatTestModel.objects.all().aggregate(RegrCount(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regrcount': 3})

    def test_regr_count_empty_result(self):
        StatTestModel.objects.all().delete()
        values = StatTestModel.objects.all().aggregate(RegrCount(y='int2', x='int1'))
        self.assertEqual(values, {'int2_int1__regrcount': 0})
