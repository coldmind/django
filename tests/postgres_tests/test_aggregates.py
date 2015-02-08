import unittest

from django.contrib.postgres.aggregates import (
    ArrayAgg, BitAnd, BoolAnd,
)
from django.db import connection
from django.test import TestCase

from .models import SimpleTestModel


@unittest.skipUnless(connection.vendor == 'postgresql', 'PostgreSQL required')
class TestAggregates(TestCase):
    fixtures = ["aggregation.json"]

    def test_array_agg_charfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': ['Foo1', 'Foo2', 'Foo3']})

    def test_array_agg_integerfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': [1, 2, 3]})

    def test_array_agg_booleanfield(self):
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': [True, False, False]})

    def test_array_agg_empty_result(self):
        SimpleTestModel.objects.all().delete()
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('char_field'))
        self.assertEqual(values, {'char_field__arrayagg': []})
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('integer_field'))
        self.assertEqual(values, {'integer_field__arrayagg': []})
        values = SimpleTestModel.objects.all().aggregate(ArrayAgg('boolean_field'))
        self.assertEqual(values, {'boolean_field__arrayagg': []})
