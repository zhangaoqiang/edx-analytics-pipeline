import datetime

from ddt import data, ddt

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateField


UNICODE_STRING = u'\u0669(\u0361\u0e4f\u032f\u0361\u0e4f)\u06f6'
UTF8_BYTE_STRING = UNICODE_STRING.encode('utf8')


@ddt
class RecordTestCase(unittest.TestCase):

    def test_single_field_pos_arg(self):
        tr = SingleFieldRecord('foo')
        self.assertEqual(tr.name, 'foo')

    def test_too_many_pos_args(self):
        with self.assertRaises(TypeError):
            SingleFieldRecord('foo', 'bar')

    def test_not_enough_pos_args(self):
        with self.assertRaises(TypeError):
            SingleFieldRecord()

    def test_incorrect_type(self):
        with self.assertRaises(ValueError):
            SingleFieldRecord(4)

    def test_none_arg(self):
        tr = SingleFieldRecord(None)
        self.assertEqual(tr.name, None)

    def test_empty_string_arg(self):
        tr = SingleFieldRecord('')
        self.assertEqual(tr.name, '')

    def test_field_order(self):
        tr = TwoFieldRecord('foo', 'bar')
        self.assertEqual(tr.name, 'foo')
        self.assertEqual(tr.value, 'bar')

    def test_class_with_other_vars(self):
        class WithOthers(Record):
            foo = 'foo'
            name = StringField()
            bar = 10

        tr = WithOthers('baz')

        self.assertEqual(tr.foo, 'foo')
        self.assertEqual(tr.name, 'baz')
        self.assertEqual(tr.bar, 10)

    def test_record_with_no_fields(self):

        class NoFields(Record):
            pass

        tr = NoFields()

    def test_initialize_with_kwargs(self):
        tr = TwoFieldRecord(value='bar', name='foo')
        self.assertEqual(tr.name, 'foo')
        self.assertEqual(tr.value, 'bar')

    def test_missing_field_kwargs(self):
        with self.assertRaises(TypeError):
            TwoFieldRecord(value='bar')

    def test_mixed_args_kwargs(self):
        tr = ThreeFieldRecord('a', 'b', third='c')
        self.assertEqual(tr.first, 'a')
        self.assertEqual(tr.second, 'b')
        self.assertEqual(tr.third, 'c')

    def test_mixed_with_missing(self):
        with self.assertRaises(TypeError):
            ThreeFieldRecord('a', second='b')

    def test_extra_kwargs(self):
        with self.assertRaises(TypeError):
            SingleFieldRecord('a', second='b')

    def test_mixed_same_arg_appears_twice(self):
        with self.assertRaises(TypeError):
            TwoFieldRecord('a', 'b', name='c')

    def test_to_string_tuple(self):
        r = SampleStruct(UNICODE_STRING, 0, datetime.date(2015, 11, 1))
        self.assertEqual(
            r.to_string_tuple(),
            (UTF8_BYTE_STRING, '0', '2015-11-01')
        )

    def test_to_string_tuple_nulls(self):
        r = SampleStruct(None, 0, None)
        self.assertEqual(
            r.to_string_tuple(),
            ('\\N', '0', '\\N')
        )

    def test_to_string_tuple_custom_nulls(self):
        r = SampleStruct(None, 0, None)
        self.assertEqual(
            r.to_string_tuple(null_value='empty'),
            ('empty', '0', 'empty')
        )

    def test_from_string_tuple(self):
        string_tuple = (UTF8_BYTE_STRING, '0', '2015-11-01')
        r = SampleStruct.from_string_tuple(string_tuple)
        self.assertEqual(r.name, UNICODE_STRING)
        self.assertEqual(r.index, 0)
        self.assertEqual(r.date, datetime.date(2015, 11, 1))

    def test_from_string_tuple_nulls(self):
        string_tuple = ('\\N', '0', '2015-11-01')
        r = SampleStruct.from_string_tuple(string_tuple)
        self.assertEqual(r.name, None)
        self.assertEqual(r.index, 0)
        self.assertEqual(r.date, datetime.date(2015, 11, 1))

    def test_from_string_tuple_custom_nulls(self):
        string_tuple = ('empty', '0', '2015-11-01')
        r = SampleStruct.from_string_tuple(string_tuple, null_value='empty')
        self.assertEqual(r.name, None)
        self.assertEqual(r.index, 0)
        self.assertEqual(r.date, datetime.date(2015, 11, 1))

    @data(
        ('foo', '0'),
        ('foo', '0', '2015-11-01', '10')
    )
    def test_from_string_tuple_length_mismatch(self, string_tuple):
        with self.assertRaises(ValueError):
            SampleStruct.from_string_tuple(string_tuple)

    @data(
        ('foo', '0', 'foo'),
        ('foo', 'bar', '2015-11-01')
    )
    def test_from_string_tuple_type_mismatch(self, string_tuple):
        with self.assertRaises(ValueError):
            SampleStruct.from_string_tuple(string_tuple)

    def test_sql_schema(self):
        self.assertEqual(
            SampleStruct.get_sql_schema(),
            [
                ('name', 'VARCHAR'),
                ('index', 'INT'),
                ('date', 'DATE')
            ]
        )

    def test_hive_schema(self):
        self.assertEqual(
            SampleStruct.get_hive_schema(),
            [
                ('name', 'STRING'),
                ('index', 'INT'),
                ('date', 'STRING')
            ]
        )

    def test_from_tsv_nulls(self):
        tsv_string = '\\N\t0\t2015-11-01\r\n'
        r = SampleStruct.from_tsv(tsv_string)
        self.assertEqual(r.name, None)
        self.assertEqual(r.index, 0)
        self.assertEqual(r.date, datetime.date(2015, 11, 1))


class SingleFieldRecord(Record):
    name = StringField()


class TwoFieldRecord(Record):
    name = StringField()
    value = StringField()


class ThreeFieldRecord(Record):
    first = StringField()
    second = StringField()
    third = StringField()


class SampleStruct(Record):
    name = StringField()
    index = IntegerField()
    date = DateField()


@ddt
class StringFieldTest(unittest.TestCase):

    @data(
        'foo',
        u'foo',
        None,
        ''
    )
    def test_validate_success(self, value):
        sf = StringField()
        self.assertEqual(len(sf.validate(value)), 0)

    @data(
        10,
        1.0,
        object(),
        False
    )
    def test_validate_error(self, value):
        sf = StringField()
        self.assertEqual(len(sf.validate(value)), 1)

    def test_non_nullable(self):
        sf = StringField(nullable=False)
        self.assertEqual(len(sf.validate(None)), 1)

    @data(
        '',
        'a',
        'bc',
        'def'
    )
    def test_length_ok(self, value):
        sf = StringField(length=3)
        self.assertEqual(len(sf.validate(value)), 0)

    @data(
        'abcd',
        'abcde'
    )
    def test_length_exceeded(self, value):
        sf = StringField(length=3)
        self.assertEqual(len(sf.validate(value)), 1)

    def test_sql_type(self):
        sf = StringField()
        self.assertEqual(sf.sql_type, 'VARCHAR')

    def test_sql_type_not_null(self):
        sf = StringField(nullable=False)
        self.assertEqual(sf.sql_type, 'VARCHAR NOT NULL')

    def test_sql_type_with_length(self):
        sf = StringField(length=10)
        self.assertEqual(sf.sql_type, 'VARCHAR(10)')

    def test_length_zero(self):
        with self.assertRaises(ValueError):
            StringField(length=0)

    def test_sql_type_with_length_not_null(self):
        sf = StringField(length=10, nullable=False)
        self.assertEqual(sf.sql_type, 'VARCHAR(10) NOT NULL')

    def test_hive_type(self):
        self.assertEqual(StringField().hive_type, 'STRING')


@ddt
class IntegerFieldTest(unittest.TestCase):

    @data(
        -1,
        10,
        None,
        0
    )
    def test_validate_success(self, value):
        f = IntegerField()
        self.assertEqual(len(f.validate(value)), 0)

    @data(
        1.0,
        'foo',
        object()
    )
    def test_validate_error(self, value):
        f = IntegerField()
        self.assertEqual(len(f.validate(value)), 1)

    def test_sql_type(self):
        self.assertEqual(IntegerField().sql_type, 'INT')

    def test_hive_type(self):
        self.assertEqual(IntegerField().hive_type, 'INT')


@ddt
class DateFieldTest(unittest.TestCase):

    @data(
        datetime.date.today(),
        None
    )
    def test_validate_success(self, value):
        f = DateField()
        self.assertEqual(len(f.validate(value)), 0)

    @data(
        0,
        False,
        1.0,
        '2015-11-01',
        object()
    )
    def test_validate_error(self, value):
        f = DateField()
        self.assertEqual(len(f.validate(value)), 1)

    def test_sql_type(self):
        self.assertEqual(DateField().sql_type, 'DATE')

    def test_hive_type(self):
        self.assertEqual(DateField().hive_type, 'STRING')
