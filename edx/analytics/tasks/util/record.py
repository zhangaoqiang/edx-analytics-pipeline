
from collections import OrderedDict
import datetime
import itertools


DEFAULT_NULL_VALUE = '\\N'


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

    def __init__(self, *args, **kwargs):
        fields = self.get_fields()

        # First process all of the positional arguments, and map them to the fields in order of field declaration.

        # Create a one-off object that will represent a NULL value. Don't use None since that may be a real value for
        # a field.
        sentinel = object()
        # Args that aren't assigned to fields will be stored here and included in the exception later.
        extra_args = []
        # Fields that had no argument mapped to them
        remaining_fields = []
        # Use izip_longest instead of zip since it allows us to detect the case when more values have been provided than
        # there are fields in the object. This case should raise a TypeError, so we need to detect it here.
        for val, field_name in itertools.izip_longest(args, fields.keys(), fillvalue=sentinel):
            if val is sentinel:
                remaining_fields.append(field_name)
            elif field_name is sentinel:
                # We have exhausted the fields and there are unconsumed arguments, raise an error later.
                extra_args.append(val)
            else:
                self.initialize_field(field_name, val)
                if field_name in kwargs:
                    raise TypeError(
                        'Multiple values provided for the same field "{0}": "{1}" and "{2}"'.format(
                            field_name, val, kwargs[field_name]
                        )
                    )

        if len(extra_args) > 0:
            raise TypeError(
                'Too many positional arguments. Unused args: {0}'.format(', '.join(extra_args))
            )

        # Now iterate through any remaining fields and try to find them in the keyword arguments.
        missing_fields = []
        for field_name in remaining_fields:
            try:
                val = kwargs.pop(field_name)
                self.initialize_field(field_name, val)
            except KeyError:
                missing_fields.append(field_name)

        if len(missing_fields) > 0:
            raise TypeError('Required fields not specified: {0}'.format(', '.join(missing_fields)))

        # Raise an error if we found any keyword arguments that weren't mapped to a field.
        if len(kwargs) > 0:
            raise TypeError('Unknown fields specified: {0}'.format(', '.join(kwargs.keys())))

    def initialize_field(self, field_name, value):
        """
        Make sure the value is compatible with the field and assign that value to the field.

        Arguments:
            field_name (string): The name of the field that is being set.
            value (object): The value to assign to the field.

        """
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
        """
        Get all of the fields in this record in order of declaration.

        Returns: An OrderedDict mapping field names to the field objects in the order they are declared in the record.
        """
        if not hasattr(cls, '_fields'):
            fields = []
            for field_name in dir(cls):
                field_obj = getattr(cls, field_name)
                if not isinstance(field_obj, Field):
                    continue

                fields.append((field_name, field_obj))

            # Field ordering matters!
            fields.sort(key=lambda t: t[1].counter)
            cls._fields = OrderedDict(fields)

        return cls._fields

    def to_string_tuple(self, null_value=DEFAULT_NULL_VALUE):
        """
        Convert the record into a tuple of UTF-8 encoded byte strings.

        This format is convenient for Luigi since it expects tuples of strings as output from reduce functions.

        Arguments:
            null_value (str): The string to use to represent None if a nullable field has a None value.

        """
        field_values = []
        for field_name, field_obj in self.get_fields().items():
            val = getattr(self, field_name)
            if val is not None:
                string_val = field_obj.serialize_to_string(val)
            else:
                string_val = null_value

            field_values.append(string_val.encode('utf8'))

        return tuple(field_values)

    @classmethod
    def from_string_tuple(cls, string_tuple, null_value=DEFAULT_NULL_VALUE):
        """
        Construct a record from an iterable of strings.

        The number of strings in the iterable must match the number of fields in the Record. Each string will be
        interpreted by the field and coerced into the appropriate type.

        Arguments:
            string_tuple (iterable): The values for the fields as strings.
            null_value (string): When an input string has this value it is set to None in the constructed Record.

        """
        fields = cls.get_fields()
        if len(string_tuple) != len(fields):
            raise ValueError('The length of the tuple of strings must exactly match the number of fields in the Redord')
        typed_field_values = []
        for str_value, field_name in zip(string_tuple, fields):
            field_obj = fields[field_name]
            if str_value != null_value:
                value = field_obj.deserialize_from_string(str_value.decode('utf8'))
            else:
                value = None

            typed_field_values.append(value)

        return cls(*typed_field_values)

    @classmethod
    def from_tsv(cls, tsv_str):
        return cls.from_string_tuple(tuple(tsv_str.rstrip('\r\n').split('\t')))

    @classmethod
    def get_sql_schema(cls):
        """
        A skeleton schema of the SQL table that could store this data.

        Returns: A list of tuples whose first element is the column name, and the second element is the type to assign
            to the column. Note that this returns SQL92 compliant types including some modifiers (NOT NULL) etc. It
            Does not include any index definitions, constraints or relationship declarations.
        """
        schema = []
        for field_name, field_obj in cls.get_fields().items():
            schema.append((field_name, field_obj.sql_type))
        return schema

    @classmethod
    def get_hive_schema(cls):
        """
        A skeleton schema of the Hive table that could store this data.

        Returns: A list of tuples whose first element is the column name, and the second element is the type to assign
            to the column. Note that Hive data types often are quite different than other SQL databases.
        """
        schema = []
        for field_name, field_obj in cls.get_fields().items():
            schema.append((field_name, field_obj.hive_type))
        return schema


class Field(object):
    """
    Represents a field within a record.
    """
    counter = 0

    def __init__(self, **kwargs):
        self.nullable = kwargs.pop('nullable', True)

        # This is a hack that lets us "see" the order the class member variables appear in the class they are declared
        # in. Sorting by this counter will allow us to order them appropriately. Note that this isn't atomic and has
        # all kinds of issues, but is funcitional and doesn't require parsing the AST or anything *more* hacky.
        self.counter = Field.counter
        Field.counter += 1

    def validate(self, value):
        """
        Determine if this value is an acceptable value for this field.

        The goal of this method is to do some trivial checks to detect problems with the data as early as possible and
        raise an error. This will prevent us from attempting to insert data into the database that will definitely cause
        errors on insertion.

        Arguments:
            value (object):

        Returns: A list of validation error strings. If the list is empty, the value is acceptable.
        """
        validation_errors = []
        if value is None and not self.nullable:
            validation_errors.append('The field cannot accept null values')
        return validation_errors

    def serialize_to_string(self, value):
        return unicode(value)

    def deserialize_from_string(self, string_value):
        return string_value

    @property
    def sql_type(self):
        base_type = self.sql_base_type
        if not self.nullable:
            base_type += ' NOT NULL'
        return base_type

    @property
    def sql_base_type(self):
        raise NotImplementedError

    @property
    def hive_type(self):
        raise NotImplementedError


class StringField(Field):

    hive_type = 'STRING'

    def __init__(self, **kwargs):
        super(StringField, self).__init__(**kwargs)
        self.length = kwargs.pop('length', None)
        if self.length is not None and self.length == 0:
            raise ValueError('Length must be greater than 0')

    def validate(self, value):
        validation_errors = super(StringField, self).validate(value)
        if value is not None:
            if not isinstance(value, basestring):
                validation_errors.append('The value is not a string')
            elif self.length and len(value) > self.length:
                validation_errors.append('The string length exceeds the maximum allowed')
        return validation_errors

    @property
    def sql_base_type(self):
        if self.length:
            return 'VARCHAR({length})'.format(length=self.length)
        else:
            return 'VARCHAR'


class IntegerField(Field):

    hive_type = sql_base_type = 'INT'

    def validate(self, value):
        validation_errors = super(IntegerField, self).validate(value)
        if value is not None and not isinstance(value, int):
            validation_errors.append('The value is not an integer')
        return validation_errors

    def deserialize_from_string(self, string_value):
        return int(string_value)


class DateField(Field):

    hive_type = 'STRING'
    sql_base_type = 'DATE'

    def validate(self, value):
        validation_errors = super(DateField, self).validate(value)
        if value is not None and not isinstance(value, datetime.date):
            validation_errors.append('The value is not a date')
        return validation_errors

    def deserialize_from_string(self, string_value):
        return datetime.date(*[int(x) for x in string_value.split('-')])
