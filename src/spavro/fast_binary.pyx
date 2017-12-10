# Copyright (C) 2017 Pluralsight LLC
'''Fast Cython extension for reading / writing and validating AVRO records.

The main edge this code has is that it parses the schema only once and creates
a reader/writer call tree from the schema shape. All reads and writes then
no longer consult the schema saving lookups.'''

import six
INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1

cdef enum cache_type:
    ct_namespace
    ct_array
    ct_map
    ct_union
    ct_skip

cdef class BaseReader(object):
    # Do not use cpdef, in order to have faster C calls.
    cdef object c_read(self, fo):
        raise NotImplementedError
    def __call__(self, fo):
        return self.c_read(fo)


cdef long long read_long(fo) except? -999999:
    '''Read a long using zig-zag binary encoding'''
    cdef:
        unsigned long long accum = 0
        unsigned long long digit = 0x80
        unsigned int shift = 0
        object read = fo.read
        bytes buf
        char c
    while (digit & 0x80):
        buf = read(1)
        c = buf[0]
        digit = c
        accum |= (digit & 0x7F) << shift
        shift += 7
    return (accum >> 1) ^ -(accum & 1)

cdef class LongReader(BaseReader):
    cdef object c_read(self, fo):
        return read_long(fo)

cdef BaseReader long_reader_singleton = LongReader()


cdef bytes read_bytes(fo):
    '''Bytes are a marker for length of bytes and then binary data'''
    return fo.read(read_long(fo))

cdef class BytesReader(BaseReader):
    cdef object c_read(self, fo):
        return read_bytes(fo)

cdef BaseReader bytes_reader_singleton = BytesReader()


cdef class NullReader(BaseReader):
    cdef object c_read(self, fo):
        """
        null is written as zero bytes
        """
        return None

cdef BaseReader null_reader_singleton = NullReader()


cdef read_boolean(fo):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    return fo.read(1) == b'\x01'

cdef class BooleanReader(BaseReader):
    cdef object c_read(self, fo):
        return read_boolean(fo)

cdef BaseReader boolean_reader_singleton = BooleanReader()


cdef float read_float(fo) except? -999999:
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    data = fo.read(4)
    cdef char* y = data
    return (<float*>y)[0]

cdef class FloatReader(BaseReader):
    cdef object c_read(self, fo):
        return read_float(fo)

cdef BaseReader float_reader_singleton = FloatReader()


cdef double read_double(fo) except? -999999:
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    data = fo.read(8)
    cdef char* y = data
    return (<double*>y)[0]

cdef class DoubleReader(BaseReader):
    cdef object c_read(self, fo):
        return read_double(fo)

cdef BaseReader double_reader_singleton = DoubleReader()


cdef unicode read_utf8(fo):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    byte_data = read_bytes(fo)
    return unicode(byte_data, "utf-8")

cdef class Utf8Reader(BaseReader):
    cdef object c_read(self, fo):
        return read_utf8(fo)

cdef BaseReader utf8_reader_singleton = Utf8Reader()


# ======================================================================
from collections import namedtuple
WriteField = namedtuple('WriteField', ['name', 'writer'])

cpdef unicode get_type(schema):
    if isinstance(schema, list):
        return u"union"
    elif isinstance(schema, dict):
        return unicode(schema['type'])  # "record"
    else:
        return unicode(schema)

cdef object primitive_types = frozenset((
    u'null', u'boolean', u'int', u'long', u'float', u'double', 
    u'bytes', u'string'
))


cdef class UnionReader(BaseReader):
    cdef tuple readers 
    def __init__(self, tuple readers):
        self.readers = readers
    cdef c_read(self, fo):
        '''Read the long index for which schema to process, then use that'''
        cdef Py_ssize_t union_index = read_long(fo)
        cdef BaseReader reader = self.readers[union_index]
        return reader.c_read(fo)

def make_union_reader(union_schema, schema_cache):
    cdef BaseReader reader
    cdef tuple schema_types = tuple(get_type(schema) for schema in union_schema)
    cdef tuple readers
    if all(schema_type in primitive_types for schema_type in schema_types):
        try:
            reader = schema_cache[(ct_union, schema_types)]
        except KeyError:
            readers = tuple(get_reader(schema, schema_cache) for schema in union_schema)
            reader = UnionReader(readers)
            schema_cache[(ct_union, schema_types)] = reader
        return reader
    readers = tuple(get_reader(schema, schema_cache) for schema in union_schema)
    reader = UnionReader(readers)
    return reader
    

cdef class ReadField(object):
    cdef object name
    cdef BaseReader reader
    cdef bint skip
    def __cinit__(self, name, reader, skip):
        self.name = name
        self.reader = reader
        self.skip = skip

cdef class RecordReader(BaseReader):
    cdef tuple fields
    def __init__(self, fields):
        self.fields = fields
    cdef c_read(self, fo):
        cdef ReadField field
        cdef dict d = dict()
        for field in self.fields:
            if not (field.skip and field.reader.c_read(fo) is None):
                d[field.name] = field.reader.c_read(fo)
        return d

def make_record_reader(schema, schema_cache):
    cdef tuple fields = tuple(
        ReadField(
            field['name'],
            get_reader(field['type'], schema_cache),
            get_type(field['type']) == 'skip'
        ) for field in schema['fields']
    )
    return RecordReader(fields)


cdef class EnumReader(BaseReader):
    cdef tuple symbols
    def __init__(self, symbols):
        self.symbols = symbols
    cdef c_read(self, fo):
        cdef Py_ssize_t i = read_long(fo)
        return self.symbols[i]

def make_enum_reader(schema, schema_cache):
    cdef tuple symbols = tuple(schema['symbols'])
    return EnumReader(symbols)


cdef class ArrayReader(BaseReader):
    cdef BaseReader item_reader
    def __init__(self, item_reader):
        self.item_reader = item_reader
    cdef c_read(self, fo):
        cdef list read_items = []
        cdef Py_ssize_t block_count = read_long(fo)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long(fo) # we don't use the block bytes count
            for i in range(block_count):
                read_items.append(self.item_reader.c_read(fo))
            block_count = read_long(fo)
        return read_items

def make_array_reader(schema, schema_cache):
    cdef BaseReader reader
    item_schema = schema['items']
    item_type = get_type(item_schema)
    if item_type in primitive_types:
        try:
            reader = schema_cache[(ct_array, item_type)]
        except KeyError:
            reader = ArrayReader(get_reader(item_schema, schema_cache))
            schema_cache[(ct_array, item_type)] = reader
        return reader
    reader = ArrayReader(get_reader(item_schema, schema_cache))
    return reader


cdef class MapReader(BaseReader):
    cdef BaseReader value_reader
    def __init__(self, value_reader):
        self.value_reader = value_reader
    cdef c_read(self, fo):
        cdef dict read_items = {}
        cdef Py_ssize_t block_count = read_long(fo)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long(fo) # we don't use the block bytes count
            for _ in range(block_count):
                key = read_utf8(fo)
                read_items[key] = self.value_reader.c_read(fo)
            block_count = read_long(fo)
        return read_items

def make_map_reader(schema, schema_cache):
    cdef BaseReader reader
    value_schema = schema['values']
    value_type = get_type(value_schema)
    if value_type in primitive_types:
        try:
            reader = schema_cache[(ct_map, value_type)]
        except KeyError:
            reader = MapReader(get_reader(value_schema, schema_cache))
            schema_cache[(ct_map, value_type)] = reader
        return reader
    reader = MapReader(get_reader(value_schema, schema_cache))
    return reader


cdef class FixedReader(BaseReader):
    cdef Py_ssize_t size
    def __init__(self, size):
        self.size = size
    cdef c_read(self, fo):
        return fo.read(self.size)

def make_fixed_reader(schema, schema_cache):
    cdef Py_ssize_t size = schema['size']
    return FixedReader(size)


def make_null_reader(schema, schema_cache):
    return null_reader_singleton

def make_string_reader(schema, schema_cache):
    return utf8_reader_singleton

def make_boolean_reader(schema, schema_cache):
    return boolean_reader_singleton

def make_double_reader(schema, schema_cache):
    return double_reader_singleton

def make_long_reader(schema, schema_cache):
    return long_reader_singleton

def make_byte_reader(schema, schema_cache):
    return bytes_reader_singleton

def make_float_reader(schema, schema_cache):
    return float_reader_singleton


cdef class SkipReader(BaseReader):
    cdef BaseReader value_reader
    def __init__(self, value_reader):
        self.value_reader = value_reader
    cdef c_read(self, fo):
        self.value_reader.c_read(fo)
        return None

def make_skip_reader(schema, schema_cache):
    # this will create a regular reader that will iterate the bytes
    # in the avro stream properly
    cdef BaseReader reader
    value_schema = schema['value']
    value_type = get_type(value_schema)
    if value_type in primitive_types:
        try:
            reader = schema_cache[(ct_skip, value_type)]
        except KeyError:
            reader = SkipReader(get_reader(value_schema, schema_cache))
            schema_cache[(ct_skip, value_type)] = reader
        return reader
    reader = SkipReader(get_reader(value_schema, schema_cache))
    return reader


cdef class DefaultReader(BaseReader):
    cdef object value
    def __init__(self, value):
        self.value = value 
    cdef c_read(self, fo):
        return self.value

def make_default_reader(schema, schema_cache):
    cdef BaseReader reader = DefaultReader(schema["value"])
    return reader


reader_type_map = {
    u'union': make_union_reader,
    u'record': make_record_reader,
    u'null': make_null_reader,
    u'string': make_string_reader,
    u'boolean': make_boolean_reader,
    u'double': make_double_reader,
    u'float': make_float_reader,
    u'long': make_long_reader,
    u'bytes': make_byte_reader,
    u'int': make_long_reader,
    u'fixed': make_fixed_reader,
    u'enum': make_enum_reader,
    u'array': make_array_reader,
    u'map': make_map_reader,
    u'skip': make_skip_reader,
    u'default': make_default_reader
}

cdef class ReaderPlaceholder(BaseReader):
    cdef BaseReader actual_reader
    def __init__(self):
        self.actual_reader = null_reader_singleton
    cdef object c_read(self, fo):
        return self.actual_reader.c_read(fo)
    
cpdef BaseReader get_reader(schema, dict schema_cache):
    cdef unicode schema_type = get_type(schema)
    cdef ReaderPlaceholder placeholder = ReaderPlaceholder()
    cdef BaseReader reader = placeholder
    
    if schema_type not in (u'record', u'fixed', u'enum'):
        try:
            maker = reader_type_map[schema_type]
        except KeyError:
            return schema_cache[(ct_namespace, schema_type)]
        return maker(schema, schema_cache)
        
    # using a placeholder because this is recursive and the reader isn't defined
    # yet and nested records might refer to this parent schema name
    namespace = schema.get('namespace')
    record_name = schema.get('name')
    if namespace:
        namspace_record_name = '.'.join([namespace, record_name])
    else:
        namspace_record_name = record_name
    schema_cache[(ct_namespace, namspace_record_name)] = reader
    reader = reader_type_map[schema_type](schema, schema_cache)
    # now that we've returned, assign the reader to the placeholder
    # so that the execution will work
    placeholder.actual_reader = reader
    return reader

# ======================================================================


cdef int write_int(outbuf, long long signed_datum) except -1:
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef:
        unsigned long long datum
        char temp_datum
    datum = (signed_datum << 1) ^ (signed_datum >> 63)
    while datum > 127:
        temp_datum = (datum & 0x7f) | 0x80
        outbuf.write((<char *>&temp_datum)[:sizeof(char)])
        datum >>= 7
    outbuf.write((<char *>&datum)[:sizeof(char)])
    return 0

write_long = write_int


cdef int write_bytes(outbuf, datum) except -1:
    """
    Bytes are encoded as a long followed by that many bytes of data. 
    """
    cdef long byte_count = len(datum)
    write_long(outbuf, byte_count)
    outbuf.write(datum)
    return 0


# except *
cdef int write_utf8(outbuf, datum) except -1:
    """
    Bytes are encoded as a long followed by that many bytes of data.
    """
    write_bytes(outbuf, datum.encode("utf-8"))
    return 0


cdef int write_float(outbuf, float datum) except -1:
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    outbuf.write((<char *>&datum)[:sizeof(float)])
    return 0


cdef int write_double(outbuf, double datum) except -1:
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    outbuf.write((<char *>&datum)[:sizeof(double)])
    return 0


cdef void write_null(outbuf, datum):
    pass


cdef int write_fixed(outbuf, datum) except -1:
    """A fixed writer writes out exactly the bytes up to a count"""
    outbuf.write(datum)
    return 0


cdef int write_boolean(outbuf, char datum) except -1:
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef char x = 1 if datum else 0
    outbuf.write((<char *>&x)[:sizeof(char)])
    return 0


avro_to_py = {
    u"string": unicode,
    u"bytes": bytes,
    u"int": int,
    u"long": int,
    u"boolean": bool,
    u"null": type(None),
    u"float": float,
    u"double": float,
    u"array": list,
    u"record": dict,
    u"enum": unicode,
    u"fixed": str,
    u"map": dict
}

py_to_avro = {
    unicode: u'string',
    bytes: u'bytes',
    int: u'int',
    long: u'long',
    bool: u'boolean',
    type(None): u'null',
    float: u'double',
    list: u'array',
    dict: u'record'
}

# ===============================
CheckField = namedtuple('CheckField', ['name', 'check'])

def get_check(schema):
    cdef unicode schema_type = get_type(schema)
    return check_type_map[schema_type](schema)


def make_record_check(schema):
    cdef list fields = [CheckField(field['name'], get_check(field['type'])) for field in schema['fields']]
    def record_check(datum):
        return isinstance(datum, dict) and all([field.check(datum.get(field.name)) for field in fields])
    return record_check


def make_enum_check(schema):
    cdef list symbols = schema['symbols']
    def enum_check(datum):
        return datum in symbols
    return enum_check


def null_check(datum):
    return datum is None

def make_null_check(schema):
    return null_check

def check_string(datum):
    return isinstance(datum, six.text_type)

def make_string_check(schema):
    return check_string


def long_check(datum):
    return isinstance(datum, six.integer_types)

def make_long_check(schema):
    return long_check
    

def boolean_check(datum):
    return isinstance(datum, bool)

def make_boolean_check(schema):
    return boolean_check


def float_check(datum):
    return isinstance(datum, six.integer_types) or isinstance(datum, float)

def make_float_check(schema):
    return float_check

def make_double_check(schema):
    return float_check


def byte_check(datum):
    return isinstance(datum, six.binary_type)

def make_byte_check(schema):
    return byte_check


def make_array_check(schema):
    item_check = get_check(schema['items'])
    def array_check(datum):
        return all(item_check(item) for item in datum)
    return array_check

def make_union_check(union_schema):
    cdef list union_checks = [get_check(schema) for schema in union_schema]
    def union_check(datum):
        return any(check(datum) for check in union_checks)
    return union_check

def make_fixed_check(schema):
    cdef int size = schema['size']
    def fixed_check(datum):
        return isinstance(datum, six.binary_type) and len(datum) == size
    return fixed_check

def make_map_check(schema):
    map_value_check = get_check(schema['values'])
    def map_check(datum):
        return isinstance(datum, dict) and all(check_string(key) 
            and map_value_check(value) for key, value in six.iteritems(datum)
        )
    return map_check

check_type_map = {
    u'union': make_union_check,
    u'record': make_record_check,
    u'null': make_null_check,
    u'string': make_string_check,
    u'boolean': make_boolean_check,
    u'double': make_double_check,
    u'float': make_float_check,
    u'long': make_long_check,
    u'bytes': make_byte_check,
    u'int': make_long_check,
    u'fixed': make_fixed_check,
    u'enum': make_enum_check,
    u'array': make_array_check,
    u'map': make_map_check
}

# ====================

def make_union_writer(union_schema, schema_cache):
    cdef list type_list = [get_type(schema) for schema in union_schema]
    # cdef dict writer_lookup
    # cdef list record_list
    cdef dict writer_lookup_dict
    cdef char simple_union
    cdef list lookup_result
    cdef long idx

    # if there's more than one kind of record in the union
    # or if there's a string, enum or fixed combined in the union
    # or there is both a record and a map in the union then it's no longer
    # simple. The reason is that reocrds and maps both correspond to python
    # dict so a simple python type lookup isn't enough to schema match.
    # enums, strings and fixed are all python data type unicode or string
    # so those won't work either when mixed
    simple_union = not(type_list.count('record') > 1 or
                      len(set(type_list) & set(['bytes', 'string', 'enum', 'fixed'])) > 1 or
                      len(set(type_list) & set(['record', 'map'])) > 1)

    if simple_union:
        writer_lookup_dict = {avro_to_py[get_type(schema)]: 
            (idx, get_writer(schema, schema_cache)) 
            for idx, schema in enumerate(union_schema)
        }
        if int in writer_lookup_dict:
            writer_lookup_dict[long] = writer_lookup_dict[int]
        # warning, this will fail if there's both a long and int in a union
        # or a float and a double in a union (which is valid but nonsensical
        # in python but valid in avro)
        def simple_writer_lookup(datum):
            return writer_lookup_dict[type(datum)]

        writer_lookup = simple_writer_lookup
    else:
        writer_lookup_dict = {}
        for idx, schema in enumerate(union_schema):
            python_type = avro_to_py[get_type(schema)]
            if python_type in writer_lookup_dict:
                writer_lookup_dict[python_type] = writer_lookup_dict[python_type] + [(idx, get_check(schema), get_writer(schema, schema_cache))]
            else:
                writer_lookup_dict[python_type] = [(idx, get_check(schema), get_writer(schema, schema_cache))]

        if int in writer_lookup_dict:
            writer_lookup_dict[long] = writer_lookup_dict[int]

        def complex_writer_lookup(datum):
            cdef:
                long idx
                list lookup_result
            lookup_result = writer_lookup_dict[type(datum)]
            if len(lookup_result) == 1:
                idx, get_check, writer = lookup_result[0]
            else:
                for idx, get_check, writer in lookup_result:
                    if get_check(datum):
                        break
                else:
                    raise TypeError("No matching schema for datum: {}".format(datum))
            return idx, writer

        writer_lookup = complex_writer_lookup

    def write_union(outbuf, datum):
        idx, data_writer = writer_lookup(datum)
        write_long(outbuf, idx)
        data_writer(outbuf, datum)
    write_union.__reduce__ = lambda: (make_union_writer, (union_schema, schema_cache))
    return write_union

def make_enum_writer(schema, schema_cache):
    cdef list symbols = schema['symbols']

    # the datum can be str or unicode?
    def write_enum(outbuf, basestring datum):
        cdef int enum_index = symbols.index(datum)
        write_int(outbuf, enum_index)
    write_enum.__reduce__ = lambda: (make_enum_writer, (schema, schema_cache))
    return write_enum


def make_record_writer(schema, schema_cache):
    cdef list fields = [WriteField(field['name'], 
        get_writer(field['type'], schema_cache)) for field in schema['fields']
    ]

    def write_record(outbuf, datum):
        for field in fields:
            field.writer(outbuf, datum.get(field.name))
    write_record.__reduce__ = lambda: (make_record_writer, (schema, schema_cache))
    return write_record


def make_array_writer(schema, schema_cache):
    item_writer = get_writer(schema['items'], schema_cache)

    def write_array(outbuf, list datum):
        cdef long item_count = len(datum)
        if item_count > 0:
            write_long(outbuf, item_count)
        for item in datum:
            item_writer(outbuf, item)
        write_long(outbuf, 0)
    write_array.__reduce__ = lambda: (make_array_writer, (schema, schema_cache))
    return write_array


def make_map_writer(schema, schema_cache):
    map_value_writer = get_writer(schema['values'], schema_cache)

    def write_map(outbuf, datum):
        cdef long item_count = len(datum)
        if item_count > 0:
            write_long(outbuf, item_count)
        for key, val in datum.iteritems():
            write_utf8(outbuf, key)
            map_value_writer(outbuf, val)
        write_long(outbuf, 0)
    write_map.__reduce__ = lambda: (make_map_writer, (schema, schema_cache))
    return write_map


cdef checked_boolean_writer(outbuf, datum):
    if not isinstance(datum, bool):
        raise TypeError("Not a boolean value")
    write_boolean(outbuf, datum)

def make_boolean_writer(schema, schema_cache):
    '''Create a boolean writer, adds a validation step before the actual
    write function'''
    return checked_boolean_writer


def make_fixed_writer(schema, schema_cache):
    '''A writer that must write X bytes defined by the schema'''
    cdef long size = schema['size']
    # note: not a char* because those are null terminated and fixed
    # has no such limitation
    def checked_write_fixed(outbuf, datum):
        if len(datum) != size:
            raise TypeError("Size Mismatch for Fixed data")
        write_fixed(outbuf, datum)
    return checked_write_fixed


cdef checked_int_write(outbuf, datum):
    if not (isinstance(datum, six.integer_types)
        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
    ):
        raise TypeError("Non integer value or overflow")
    write_long(outbuf, datum)

def make_int_writer(schema, schema_cache):
    '''Create a int writer, adds a validation step before the actual
    write function to make sure the int value doesn't overflow'''
    return checked_int_write


cdef checked_long_write(outbuf, datum):
    if not (isinstance(datum, six.integer_types)
        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
    ):
        raise TypeError("Non integer value or overflow")
    write_long(outbuf, datum)

def make_long_writer(schema, schema_cache):
    '''Create a long writer, adds a validation step before the actual
    write function to make sure the long value doesn't overflow'''
    return checked_long_write


def make_string_writer(schema, schema_cache):
    # Adding exception propagation to write_utf8 means that we do no have to
    # check the type; write_utf8 will raise an exception if the
    # encode('utf-8') method does not work.
    return write_utf8


def make_byte_writer(schema, schema_cache):
    return write_bytes


def make_float_writer(schema, schema_cache):
    return write_float


def make_double_writer(schema, schema_cache):
    return write_double


def make_null_writer(schema, schema_cache):
    return write_null


# writer
writer_type_map = {
    u'union': make_union_writer,
    u'record': make_record_writer,
    u'null': make_null_writer,
    u'string': make_string_writer,
    u'boolean': make_boolean_writer,
    u'double': make_double_writer,
    u'float': make_float_writer,
    u'long': make_long_writer,
    u'bytes': make_byte_writer,
    u'int': make_int_writer,
    u'fixed': make_fixed_writer,
    u'enum': make_enum_writer,
    u'array': make_array_writer,
    u'map': make_map_writer
}


cdef class WriterPlaceholder(object):
    cdef object writer
    def __init__(self):
        self.writer = None

    def __call__(self, fo, val):
        return self.writer(fo, val)


def get_writer(schema, schema_cache, top=False):
    cdef unicode schema_type = get_type(schema)
    if schema_type in (u'record', u'fixed', u'enum'):
        placeholder = WriterPlaceholder()
        # using a placeholder because this is recursive and the reader isn't defined
        # yet and nested records might refer to this parent schema name
        namespace = schema.get('namespace')
        name = schema.get('name')
        if namespace:
           fullname = '.'.join([namespace, name])
        else:
            fullname = name
        schema_cache[fullname] = placeholder
        writer = writer_type_map[schema_type](schema, schema_cache)
        # now that we've returned, assign the reader to the placeholder
        # so that the execution will work
        placeholder.writer = writer
        return writer
    if top:
        # If schema is a single primitive type, wrap with placeholder so that
        # one has a Python callable, instead of just a cdef.
        placeholder = WriterPlaceholder()
        writer = writer_type_map[schema_type](schema, schema_cache)
        placeholder.writer = writer
        return writer
    try:
        writer_func = writer_type_map[schema_type]
    except KeyError:
        return schema_cache[schema_type]
    return writer_func(schema, schema_cache)





import struct
from binascii import crc32


class FastBinaryEncoder(object):
    """Write leaf values."""
    def __init__(self, writer):
        """
        writer is a Python object on which we can call write.
        """
        self.writer = writer

    def write(self, datum):
        self.writer.write(datum)

    def write_null(self, datum):
        pass

    def write_boolean(self, datum):
        write_boolean(self.writer, datum)

    def write_int(self, datum):
        write_int(self.writer, datum)

    def write_long(self, datum):
        write_long(self.writer, datum)

    def write_float(self, datum):
        write_float(self.writer, datum)

    def write_double(self, datum):
        write_double(self.writer, datum)

    def write_bytes(self, datum):
        write_bytes(self.writer, datum)

    def write_utf8(self, datum):
        write_utf8(self.writer, datum)

    def write_crc32(self, bytes):
        """
        A 4-byte, big-endian CRC32 checksum
        """
        self.writer.write(struct.pack("!I", crc32(bytes) & 0xffffffff))



class FastBinaryDecoder(object):
    """Read leaf values."""
    def __init__(self, reader):
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self.reader = reader

    def read(self, n):
        return self.reader.read(n)

    def read_null(self):
        return None

    def read_boolean(self):
        return read_boolean(self.reader)

    def read_int(self):
        return read_long(self.reader)

    def read_long(self):
        return read_long(self.reader)

    def read_float(self):
        return read_float(self.reader)

    def read_double(self):
        return read_double(self.reader)

    def read_bytes(self):
        return read_bytes(self.reader)

    def read_utf8(self):
        return read_utf8(self.reader)

    def check_crc32(self, bytes):
        checksum = struct.unpack("!I", self.reader.read(4))[0]
        if crc32(bytes) & 0xffffffff != checksum:
            raise RuntimeError("Checksum failure")

    def skip_null(self):
        pass

    def skip_boolean(self):
        self.reader.read(1)

    def skip_int(self):
        read_long(self.reader)

    def skip_long(self):
        read_long(self.reader)

    def skip_float(self):
        read_float(self.reader)

    def skip_double(self):
        read_double(self.reader)

    def skip_bytes(self):
        read_bytes(self.reader)

    def skip_utf8(self):
        read_utf8(self.reader)

    def skip(self, n):
        self.reader.seek(self.reader.tell() + n)
