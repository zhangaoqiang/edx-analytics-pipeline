from collections import OrderedDict
import datetime


class Record(object):
    """
    Represents a strongly typed record that can be stored in various storage engines and processed by Map Reduce jobs.

    The goal is to represent the schema in a way that allows us to generate the schemas for various systems (Hive,
    Vertica, MySQL etc) as well as use that schema to serialize and deserialize the record in a variety of formats
    for Map Reduce purposes.

    Once the record is deserialized by the python code it can be used much like a namedtuple or other similar simple
    structure. It is intended to be immutable after initialization, however that can be bypassed relatively simply if
    someone wants to be rebellious.

    We could have used a more complex ORM, but decided that they weren't worth the complexity, particularly given the
    fact that we would have to implement a bunch of customization and mapping logic.

    When subclassing, the order of fields is very important!

    This class ->

        class MyRecord(Record):
            first_field = StringField()
            second_field = IntegerField()

    Will exhibit different behavior than this class ->

        class MyRecord(Record):
            second_field = IntegerField()
            first_field = StringField()
    """

    # This string will be used in place of the string "None" which is the python default for unicode(None). This
    # particular string is the default string used by Hive to represent NULL in a string representation, which is
    # convenient for us.
    null_value_string = '\\N'

    def __init__(self, *args, **kwargs):
        fields = self.get_fields()
        field_iter = iter(fields.keys())

        # First process all of the positional arguments, and map them to the fields in order of declaration.
        for val, field_name in zip(args, field_iter):
            self.initialize_field(field_name, val)
            kwargs.pop(field_name, None)

        # Now iterate through any remaining fields and try to find them in the keyword arguments.
        for field_name in field_iter:
            try:
                val = kwargs.pop(field_name)
                self.initialize_field(field_name, val)
            except KeyError:
                field_obj = fields[field_name]
                if hasattr(field_obj, 'default'):
                    self.initialize_field(field_name, field_obj.default)
                else:
                    raise TypeError('Required field "{0}" not specified'.format(field_name))

        # Raise an exception if we found any keyword arguments that weren't mapped to a field.
        if len(kwargs) > 0:
            raise TypeError('Unknown fields specified: {0}'.format(kwargs.keys()))

        # Lock the object to prevent further modification.
        self._initialized = True

    def initialize_field(self, field_name, value):
        field_obj = self.get_fields()[field_name]
        validation_errors = field_obj.validate(value)
        if len(validation_errors) > 0:
            raise ValueError('Unable to assign the value "{value}" to the field named "{name}": {errors}'.format(
                value=value,
                name=field_name,
                errors=str(validation_errors)
            ))
        else:
            setattr(self, field_name, value)

    @classmethod
    def get_fields(cls):
        if not hasattr(cls, '_fields'):
            fields = []
            for field_name in dir(cls):
                field_obj = getattr(cls, field_name)
                if not isinstance(field_obj, Field):
                    continue

                field_obj.null_value_string = cls.null_value_string
                fields.append((field_name, field_obj))

            # Field ordering matters!
            fields.sort(key=lambda t: t[1].counter)
            cls._fields = OrderedDict(fields)

        return cls._fields

    @classmethod
    def from_tsv(cls, tsv_str):
        return cls.from_string_tuple(tuple(tsv_str.rstrip('\r\n').split('\t')))

    def to_tsv(self):
        return '\t'.join(self.to_string_tuple())

    @classmethod
    def from_string_tuple(cls, t):
        fields = cls.get_fields()
        typed_field_values = []
        for str_value, field_name in zip(t, fields):
            field_obj = fields[field_name]
            typed_field_values.append(field_obj.value_from_nullable_string(str_value.decode('utf8')))

        return cls(*typed_field_values)

    def to_string_tuple(self):
        field_values = []
        for field_name, field_obj in self.get_fields().items():
            val = getattr(self, field_name)
            field_values.append(field_obj.value_to_nullable_string(val).encode('utf8'))

        return tuple(field_values)

    @classmethod
    def to_schema(cls, schema_type):
        schema = []
        for field_name, field_obj in cls.get_fields().items():
            schema.append((field_name, getattr(field_obj, schema_type + '_type')))
        return schema

    @classmethod
    def to_hive_schema(cls):
        return cls.to_schema('hive')

    @classmethod
    def to_sql_schema(cls):
        return cls.to_schema('sql')

    def __setattr__(self, key, value):
        if hasattr(self, '_initialized'):
            raise TypeError('Records are intended to be immutable')
        else:
            super(Record, self).__setattr__(key, value)

    def __delattr__(self, item):
        if hasattr(self, '_initialized'):
            raise TypeError('Records are intended to be immutable')
        else:
            super(Record, self).__delattr__(item)


class Field(object):
    counter = 0

    def __init__(self, **kwargs):
        self.null_value_string = Record.null_value_string

        if 'default' in kwargs:
            self.default = kwargs['default']

        self.nullable = kwargs.pop('nullable', True)

        # This is a hack that lets us "see" the order the class member variables appear in the class they are declared
        # in. Sorting by this counter will allow us to order them appropriately. Note that this isn't atomic and has
        # all kinds of issues, but is funcitional and doesn't require parsing the AST or anything *more* hacky.
        self.counter = Field.counter
        Field.counter += 1

    def value_from_nullable_string(self, str_value):
        if str_value == self.null_value_string:
            return None
        else:
            return self.value_from_string(str_value)

    def value_to_nullable_string(self, value):
        if value is None:
            return self.null_value_string
        else:
            return self.value_to_string(value)

    def validate(self, value):
        validation_errors = []
        if value is None and not self.nullable:
            validation_errors.append('The field cannot accept null values.')
        return validation_errors

    def value_to_string(self, value):
        return unicode(value)

    def value_from_string(self, str_value):
        raise NotImplementedError

    @property
    def hive_type(self):
        raise NotImplementedError

    @property
    def sql_type(self):
        raise NotImplementedError

    def apply_sql_modifiers(self, base_type):
        if not self.nullable:
            return base_type + ' NOT NULL'
        else:
            return base_type


class StringField(Field):

    def __init__(self, length=None, **kwargs):
        super(StringField, self).__init__(**kwargs)
        self.length = length

    def value_to_string(self, value):
        return unicode(value)

    def value_from_string(self, str_value):
        return str_value

    def validate(self, value):
        validation_errors = super(StringField, self).validate(value)
        if self.length and len(value) > self.length:
            validation_errors.append('The string length exceeds the maximum allowed.')
        return validation_errors

    @property
    def hive_type(self):
        return 'STRING'

    @property
    def sql_type(self):
        if self.length:
            base_type = 'VARCHAR({length})'.format(length=self.length)
        else:
            base_type = 'VARCHAR'

        return self.apply_sql_modifiers(base_type)


class IntegerField(Field):

    @property
    def hive_type(self):
        return 'INT'

    @property
    def sql_type(self):
        return self.apply_sql_modifiers('INT')

    @classmethod
    def value_from_string(cls, str_value):
        return int(str_value)


class DateField(Field):

    @property
    def hive_type(self):
        # hive doesn't have strong support for date objects, they have to be stored as strings and processed by UDFs
        return 'STRING'

    @property
    def sql_type(self):
        return self.apply_sql_modifiers('DATE')

    @classmethod
    def value_to_string(cls, date_obj):
        return date_obj.isoformat()

    @classmethod
    def value_from_string(cls, str_value):
        return datetime.date(*[int(x) for x in str_value.split('-')])
