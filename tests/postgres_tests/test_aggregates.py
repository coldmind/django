import unittest

from django.contrib.postgres.aggregates import (
    ArrayAgg, BitAnd, BitOr, BoolAnd, BoolOr,
)
from django.db import connection
from django.test import TestCase

from .models import SimpleTestModel


@unittest.skipUnless(connection.vendor == 'postgresql', 'PostgreSQL required')
class TestAggregates(TestCase):
    fixtures = ["aggregation.json"]

    def test_array_agg_charfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': ['Foo1', 'Foo2', 'Foo3', 'Foo4']})

    def test_array_agg_integerfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': [0, 1, 2, 0]})

    def test_array_agg_booleanfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': [True, False, False, True]})

    def test_array_agg_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': []})
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': []})
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': []})

    def test_bit_and_general(self):
        values = SimpleTestModel.objects.filter(
            integer_field__in=[0, 1]).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 0})

    def test_bit_and_on_only_true_values(self):
        values = SimpleTestModel.objects.filter(
            integer_field=1).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 1})

    def test_bit_and_on_only_false_values(self):
        values = SimpleTestModel.objects.filter(
            integer_field=0).aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': 0})

    def test_bit_and_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(BitAnd('integer_field'))
        self.assertEqual(values, {'integer_field__bitand': None})

    def test_bit_or_general(self):
        values = SimpleTestModel.objects.filter(
            integer_field__in=[0, 1]).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 1})

    def test_bit_or_on_only_true_values(self):
        values = SimpleTestModel.objects.filter(
            integer_field=1).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 1})

    def test_bit_or_on_only_false_values(self):
        values = SimpleTestModel.objects.filter(
            integer_field=0).aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': 0})

    def test_bit_or_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(BitOr('integer_field'))
        self.assertEqual(values, {'integer_field__bitor': None})

    def test_bool_and_general(self):
        values = SimpleTestModel.objects.all().aggregate(BoolAnd('boolean_field'))
        self.assertEqual(values, {'boolean_field__booland': False})

    def test_bool_and_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(BoolAnd('boolean_field'))
        self.assertEqual(values, {'boolean_field__booland': None})

    def test_bool_or_general(self):
        values = SimpleTestModel.objects.all().aggregate(BoolOr('boolean_field'))
        self.assertEqual(values, {'boolean_field__boolor': True})

    def test_bool_or_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(BoolOr('boolean_field'))
        self.assertEqual(values, {'boolean_field__boolor': None})
