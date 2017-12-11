# Copyright (C) 2017 Pluralsight LLC
'''Fast Cython extension for reading / writing and validating AVRO records.

The main edge this code has is that it parses the schema only once and creates
a reader/writer call tree from the schema shape. All reads and writes then
no longer consult the schema saving lookups.'''

import six
from spavro.exceptions import AvroTypeException
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

cdef class BaseSerDe(object):
    # Do not use cpdef, in order to have faster C calls.
    cdef object c_read(self, fo):
        raise NotImplementedError
    cdef int c_write(self, fo, datum) except -1:
        raise NotImplementedError
    def read(self, fo):
        return self.c_read(fo)
    def write(self, fo, datum):
        self.c_write(fo, datum)


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

cdef class LongSerDe(BaseSerDe):
    cdef object c_read(self, fo):
        return read_long(fo)
    cdef int c_write(self, fo, datum) except -1:
        return checked_long_write(fo, datum)

cdef class IntSerDe(BaseSerDe):
    cdef object c_read(self, fo):
        return read_long(fo)
    cdef int c_write(self, fo, datum) except -1:
        return checked_int_write(fo, datum)

cdef BaseSerDe long_serde_singleton = LongSerDe()
cdef BaseSerDe int_serde_singleton = IntSerDe()


cdef bytes read_bytes(fo):
    '''Bytes are a marker for length of bytes and then binary data'''
    return fo.read(read_long(fo))

cdef class BytesSerDe(BaseSerDe):
    cdef object c_read(self, fo):
        return read_bytes(fo)
    cdef int c_write(self, fo, datum) except -1:
        if isinstance(datum, bytes):
            return write_bytes(fo, datum)
        return write_utf8(fo, datum)

cdef BaseSerDe bytes_serde_singleton = BytesSerDe()


cdef class NullSerDe(BaseSerDe):
    cdef object c_read(self, fo):
        """
        null is written as zero bytes
        """
        return None
    cdef int c_write(self, fo, datum) except -1:
        return 0

cdef BaseSerDe null_serde_singleton = NullSerDe()


cdef read_boolean(fo):
    """
    a boolean is written as a single byte 
    whose value is either 0 (false) or 1 (true).
    """
    return fo.read(1) != b'\x00' # Lenient: any non-zero value is true

cdef class BooleanSerDe(BaseSerDe):
    cdef object c_read(self, fo):
        return read_boolean(fo)
    cdef int c_write(self, fo, datum) except -1:
        return checked_boolean_writer(fo, datum)

cdef BaseSerDe boolean_serde_singleton = BooleanSerDe()


from struct import Struct
# Make float/double portable by using struct module.

cdef class FloatSerDe(BaseSerDe):
    cdef object pack
    cdef object unpack
    def __init__(self):
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        s = Struct('<f')
        self.pack = s.pack
        self.unpack = s.unpack
    cdef object c_read(self, fo):
        return self.unpack(fo.read(4))[0]
    cdef int c_write(self, fo, datum) except -1:
        fo.write(self.pack(datum))
        return 0

cdef class DoubleSerDe(BaseSerDe):
    cdef object pack
    cdef object unpack
    def __init__(self):
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        s = Struct('<d')
        self.pack = s.pack
        self.unpack = s.unpack
    cdef object c_read(self, fo):
        return self.unpack(fo.read(8))[0]
    cdef int c_write(self, fo, datum) except -1:
        fo.write(self.pack(datum))
        return 0

cdef BaseSerDe float_serde_singleton = FloatSerDe()
cdef BaseSerDe double_serde_singleton = DoubleSerDe()


from cpython.unicode cimport PyUnicode_FromEncodedObject
cdef unicode read_utf8(fo):
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    cdef bytes byte_data = read_bytes(fo)
    return PyUnicode_FromEncodedObject(byte_data, "utf-8", "replace")

cdef class Utf8SerDe(BaseSerDe):
    cdef object c_read(self, fo):
        cdef unicode obj = read_utf8(fo)
        return obj
    cdef int c_write(self, fo, datum) except -1:
        return write_utf8(fo, datum)

cdef BaseSerDe utf8_serde_singleton = Utf8SerDe()


# ======================================================================

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


cdef class UnionSerDe(BaseSerDe):
    cdef object schema
    cdef tuple readers 
    cdef dict writer_lookup_dict
    cdef bint simple
    def __init__(self, schema, tuple readers=None, dict writer_lookup_dict=None, simple=True):
        self.schema = schema
        self.readers = readers
        self.writer_lookup_dict = writer_lookup_dict
        self.simple = simple
    cdef c_read(self, fo):
        '''Read the long index for which schema to process, then use that'''
        cdef Py_ssize_t union_index = read_long(fo)
        cdef BaseSerDe reader = self.readers[union_index]
        return reader.c_read(fo)
    cdef int c_write(self, fo, datum) except -1:
        cdef Py_ssize_t idx
        cdef BaseSerDe writer
        cdef list lookup_result
        if self.simple:
            idx, writer = self.writer_lookup_dict[type(datum)]
        else:
            lookup_result = self.writer_lookup_dict[type(datum)]
            if len(lookup_result) == 1:
                idx, get_check, writer = lookup_result[0]
            else:
                for idx, get_check, writer in lookup_result:
                    if get_check(datum):
                        break
                else:
                    raise AvroTypeException(self.schema, datum)
        write_long(fo, idx)
        return writer.c_write(fo, datum)


def make_union_reader(union_schema, schema_cache):
    cdef BaseSerDe reader
    cdef tuple schema_types = tuple(get_type(schema) for schema in union_schema)
    cdef tuple readers
    if all(schema_type in primitive_types for schema_type in schema_types):
        try:
            reader = schema_cache[(ct_union, schema_types)]
        except KeyError:
            readers = tuple(get_reader(schema, schema_cache) for schema in union_schema)
            reader = UnionSerDe(union_schema, readers)
            schema_cache[(ct_union, schema_types)] = reader
        return reader
    readers = tuple(get_reader(schema, schema_cache) for schema in union_schema)
    reader = UnionSerDe(union_schema, readers)
    return reader
    

cdef class RecordField(object):
    cdef object name
    cdef BaseSerDe serde
    cdef bint skip
    def __cinit__(self, name, serde, skip):
        self.name = name
        self.serde = serde
        self.skip = skip

cdef class RecordSerDe(BaseSerDe):
    cdef tuple fields
    def __init__(self, fields):
        self.fields = fields
    cdef c_read(self, fo):
        cdef RecordField field
        cdef dict d = dict()
        for field in self.fields:
            if not (field.skip and field.serde.c_read(fo) is None):
                d[field.name] = field.serde.c_read(fo)
        return d
    cdef int c_write(self, fo, datum) except -1:
        cdef RecordField field
        cdef object value
        for field in self.fields:
            try:
                value = datum[field.name]
            except KeyError:
                value = None
            field.serde.c_write(fo, value)
        return 0

def make_record_reader(schema, schema_cache):
    cdef tuple fields = tuple(
        RecordField(
            field['name'],
            get_reader(field['type'], schema_cache),
            get_type(field['type']) == 'skip'
        ) for field in schema['fields']
    )
    return RecordSerDe(fields)


cdef class EnumSerDe(BaseSerDe):
    cdef tuple symbols
    def __init__(self, symbols):
        self.symbols = symbols
    cdef c_read(self, fo):
        cdef Py_ssize_t i = read_long(fo)
        return self.symbols[i]
    cdef int c_write(self, fo, datum) except -1:
        cdef Py_ssize_t i = self.symbols.index(datum)
        write_long(fo, i)
        return 0

def make_enum_reader(schema, schema_cache):
    cdef tuple symbols = tuple(schema['symbols'])
    return EnumSerDe(symbols)


cdef class ArraySerDe(BaseSerDe):
    cdef BaseSerDe item_serde
    def __init__(self, item_serde):
        self.item_serde = item_serde
    cdef c_read(self, fo):
        cdef list read_items = []
        cdef Py_ssize_t block_count = read_long(fo)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long(fo) # we don't use the block bytes count
            for i in range(block_count):
                read_items.append(self.item_serde.c_read(fo))
            block_count = read_long(fo)
        return read_items
    cdef int c_write(self, fo, datum) except -1:
        cdef Py_ssize_t item_count = len(datum)
        if item_count > 0:
            write_long(fo, item_count)
        for item in datum:
            self.item_serde.c_write(fo, item)
        write_long(fo, 0)


def make_array_reader(schema, schema_cache):
    cdef BaseSerDe reader
    item_schema = schema['items']
    item_type = get_type(item_schema)
    if item_type in primitive_types:
        try:
            reader = schema_cache[(ct_array, item_type)]
        except KeyError:
            reader = ArraySerDe(get_reader(item_schema, schema_cache))
            schema_cache[(ct_array, item_type)] = reader
        return reader
    reader = ArraySerDe(get_reader(item_schema, schema_cache))
    return reader


cdef class MapSerDe(BaseSerDe):
    cdef BaseSerDe value_serde
    def __init__(self, value_serde):
        self.value_serde = value_serde
    cdef c_read(self, fo):
        cdef dict read_items = dict()
        cdef unicode key
        cdef object value
        cdef Py_ssize_t block_count = read_long(fo)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long(fo) # we don't use the block bytes count
            for _ in range(block_count):
                key = read_utf8(fo)
                value = self.value_serde.c_read(fo)
                read_items[key] = value 
            block_count = read_long(fo)
        return read_items
    cdef int c_write(self, fo, datum) except -1:
        cdef Py_ssize_t item_count
        cdef unicode key
        if datum is not None:
            item_count = len(datum)
            if item_count > 0:
                write_long(fo, item_count)
                for key, value in six.iteritems(datum):
                    write_utf8(fo, key)
                    self.value_serde.c_write(fo, value)
        return write_long(fo, 0)


def make_map_reader(schema, schema_cache):
    cdef BaseSerDe reader
    value_schema = schema['values']
    value_type = get_type(value_schema)
    if value_type in primitive_types:
        try:
            reader = schema_cache[(ct_map, value_type)]
        except KeyError:
            reader = MapSerDe(get_reader(value_schema, schema_cache))
            schema_cache[(ct_map, value_type)] = reader
        return reader
    reader = MapSerDe(get_reader(value_schema, schema_cache))
    return reader


cdef class FixedSerDe(BaseSerDe):
    cdef Py_ssize_t size
    cdef object schema
    def __init__(self, size, schema):
        self.size = size
        self.schema = schema
    cdef c_read(self, fo):
        return fo.read(self.size)
    cdef int c_write(self, fo, datum) except -1:
        cdef bytes bytes_data = datum
        if len(datum) != self.size:
            raise AvroTypeException(self.schema, datum)
        fo.write(bytes_data)
        return 0


def make_fixed_reader(schema, schema_cache):
    cdef Py_ssize_t size = schema['size']
    return FixedSerDe(size, schema)


def make_null_reader(schema, schema_cache):
    return null_serde_singleton

def make_string_reader(schema, schema_cache):
    return utf8_serde_singleton

def make_boolean_reader(schema, schema_cache):
    return boolean_serde_singleton

def make_double_reader(schema, schema_cache):
    return double_serde_singleton

def make_long_reader(schema, schema_cache):
    return long_serde_singleton

def make_byte_reader(schema, schema_cache):
    return bytes_serde_singleton

def make_float_reader(schema, schema_cache):
    return float_serde_singleton


cdef class SkipReader(BaseSerDe):
    cdef BaseSerDe value_reader
    def __init__(self, value_reader):
        self.value_reader = value_reader
    cdef c_read(self, fo):
        self.value_reader.c_read(fo)
        return None

def make_skip_reader(schema, schema_cache):
    # this will create a regular reader that will iterate the bytes
    # in the avro stream properly
    cdef BaseSerDe reader
    value_schema = schema['value']
    if value_schema == u'string':
        value_schema = u'bytes' # Use bytes reader, skip converting to unicode
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


cdef class DefaultReader(BaseSerDe):
    cdef object value
    def __init__(self, value):
        self.value = value 
    cdef c_read(self, fo):
        return self.value

def make_default_reader(schema, schema_cache):
    cdef BaseSerDe reader = DefaultReader(schema["value"])
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

cdef class PlaceholderSerDe(BaseSerDe):
    cdef BaseSerDe serde
    def __init__(self):
        self.serde = null_serde_singleton
    cdef object c_read(self, fo):
        return self.serde.c_read(fo)
    cdef int c_write(self, fo, datum) except -1:
        return self.serde.c_write(fo, datum)
    
cpdef BaseSerDe get_reader(schema, dict schema_cache):
    cdef unicode schema_type = get_type(schema)
    cdef PlaceholderSerDe placeholder = PlaceholderSerDe()
    cdef BaseSerDe reader = placeholder
    
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
    placeholder.serde = reader
    return reader

# ======================================================================


from cpython cimport PyBytes_FromStringAndSize
cdef int write_long(outbuf, long long signed_datum) except -1:
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef:
        unsigned long long datum
        char buf[12]
        Py_ssize_t i = 0
        bytes bobj
    datum = (signed_datum << 1) ^ (signed_datum >> 63)
    while datum > 127:
        buf[i] = (datum & 0x7f) | 0x80
        i += 1
        datum >>= 7
    buf[i] = datum
    i += 1
    bobj = PyBytes_FromStringAndSize(buf, i)
    outbuf.write(bobj)
    return 0


cdef int write_bytes(outbuf, bytes datum) except -1:
    """
    Bytes are encoded as a long followed by that many bytes of data. 
    """
    cdef Py_ssize_t byte_count = len(datum)
    write_long(outbuf, byte_count)
    outbuf.write(datum)
    return 0


# except *
from cpython.unicode cimport PyUnicode_AsUTF8String
# Alternative would be to use PyUnicode_AsEncodedString with 'replace' handler
cdef int write_utf8(outbuf, unicode datum) except -1:
    """
    Bytes are encoded as a long followed by that many bytes of data.
    """
    cdef bytes bobj = PyUnicode_AsUTF8String(datum)
    write_bytes(outbuf, bobj)
    return 0


cdef int write_boolean(outbuf, bint datum) except -1:
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef bytes x = b'\x01' if datum else b'\x00'
    outbuf.write(x)
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
from collections import namedtuple
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
    cdef tuple schema_types = tuple(get_type(schema) for schema in union_schema)
    cdef dict writer_lookup_dict
    cdef BaseSerDe writer
    cdef bint simple_union
    cdef Py_ssize_t idx

    # if there's more than one kind of record in the union
    # or if there's a string, enum or fixed combined in the union
    # or there is both a record and a map in the union then it's no longer
    # simple. The reason is that reocrds and maps both correspond to python
    # dict so a simple python type lookup isn't enough to schema match.
    # enums, strings and fixed are all python data type unicode or string
    # so those won't work either when mixed
    simple_union = not(schema_types.count('record') > 1 or
                      len(set(schema_types) & set(['bytes', 'string', 'enum', 'fixed'])) > 1 or
                      len(set(schema_types) & set(['record', 'map'])) > 1)

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
        if all(schema_type in primitive_types for schema_type in schema_types):
            try:
                writer = schema_cache[(ct_union, schema_types)]
            except:
                writer = UnionSerDe(union_schema, None, writer_lookup_dict, simple_union)
                schema_cache[(ct_union, schema_types)] = writer
        else:
            writer = UnionSerDe(union_schema, None, writer_lookup_dict, simple_union)
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
        writer = UnionSerDe(union_schema, None, writer_lookup_dict, simple_union)
    
    return writer


def make_enum_writer(schema, schema_cache):
    cdef tuple symbols = tuple(schema['symbols'])
    return EnumSerDe(symbols)


def make_record_writer(schema, schema_cache):
    cdef tuple fields = tuple(
        RecordField(
            field['name'], 
            get_writer(field['type'], schema_cache),
            0
        ) for field in schema['fields']
    )
    return RecordSerDe(fields)


def make_array_writer(schema, schema_cache):
    cdef BaseSerDe writer
    item_schema = schema['items']
    item_type = get_type(item_schema)
    if item_type in primitive_types:
        try:
            writer = schema_cache[(ct_array, item_type)]
        except KeyError:
            writer = ArraySerDe(get_writer(item_schema, schema_cache))
            schema_cache[(ct_array, item_type)] = writer
        return writer
    writer = ArraySerDe(get_writer(item_schema, schema_cache))
    return writer


def make_map_writer(schema, schema_cache):
    cdef BaseSerDe writer
    value_schema = schema['values']
    value_type = get_type(value_schema)
    if value_type in primitive_types:
        try:
            writer = schema_cache[(ct_map, value_type)]
        except KeyError:
            writer = MapSerDe(get_writer(value_schema, schema_cache))
            schema_cache[(ct_map, value_type)] = writer
        return writer
    writer = MapSerDe(get_writer(value_schema, schema_cache))
    return writer


cdef int checked_boolean_writer(outbuf, datum) except -1:
    write_boolean(outbuf, bool(datum))
    return 0

def make_boolean_writer(schema, schema_cache):
    '''Create a boolean writer, adds a validation step before the actual
    write function'''
    return boolean_serde_singleton


def make_fixed_writer(schema, schema_cache):
    '''A writer that must write X bytes defined by the schema'''
    cdef Py_ssize_t size = schema['size']
    return FixedSerDe(size, schema)


cdef int checked_int_write(outbuf, datum) except -1:
    if not (isinstance(datum, six.integer_types)
        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
    ):
        raise AvroTypeException(u'int', datum)
    write_long(outbuf, datum)
    return 0

def make_int_writer(schema, schema_cache):
    '''Create a int writer, adds a validation step before the actual
    write function to make sure the int value doesn't overflow'''
    return int_serde_singleton


cdef int checked_long_write(outbuf, datum) except -1:
    if not (isinstance(datum, six.integer_types)
        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
    ):
        raise AvroTypeException(u'long', datum)
    write_long(outbuf, datum)
    return 0

def make_long_writer(schema, schema_cache):
    '''Create a long writer, adds a validation step before the actual
    write function to make sure the long value doesn't overflow'''
    return long_serde_singleton


def make_string_writer(schema, schema_cache):
    # Adding exception propagation to write_utf8 means that we do no have to
    # check the type; write_utf8 will raise an exception if the
    # encode('utf-8') method does not work.
    return utf8_serde_singleton


def make_byte_writer(schema, schema_cache):
    return bytes_serde_singleton


def make_float_writer(schema, schema_cache):
    return float_serde_singleton


def make_double_writer(schema, schema_cache):
    return double_serde_singleton


def make_null_writer(schema, schema_cache):
    return null_serde_singleton


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

cpdef get_writer(schema, schema_cache):
    cdef unicode schema_type = get_type(schema)
    cdef PlaceholderSerDe placeholder = PlaceholderSerDe()
    cdef BaseSerDe writer = placeholder
    
    if schema_type not in (u'record', u'fixed', u'enum'):
        try:
            maker = writer_type_map[schema_type]
        except KeyError:
            return schema_cache[(ct_namespace, schema_type)]
        return maker(schema, schema_cache)

    # using a placeholder because this is recursive and the reader isn't defined
    # yet and nested records might refer to this parent schema name
    namespace = schema.get('namespace')
    name = schema.get('name')
    if namespace:
        fullname = '.'.join([namespace, name])
    else:
        fullname = name
    schema_cache[(ct_namespace, fullname)] = writer
    writer = writer_type_map[schema_type](schema, schema_cache)
    # now that we've returned, assign the reader to the placeholder
    # so that the execution will work
    placeholder.serde = writer
    return writer




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
        checked_boolean_writer(self.writer, datum)

    def write_int(self, datum):
        checked_int_write(self.writer, datum)

    def write_long(self, datum):
        checked_long_write(self.writer, datum)

    def write_float(self, datum):
        float_serde_singleton.c_write(self.writer, datum)

    def write_double(self, datum):
        double_serde_singleton.c_write(self.writer, datum)

    def write_bytes(self, datum):
        bytes_serde_singleton.c_write(self.writer, datum)

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
        return float_serde_singleton.c_read(self.reader)

    def read_double(self):
        return double_serde_singleton.c_read(self.reader)

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
        self.reader.read(4)

    def skip_double(self):
        self.reader.read(8)

    def skip_bytes(self):
        read_bytes(self.reader)

    def skip_utf8(self):
        read_bytes(self.reader)

    def skip(self, n):
        self.reader.seek(n, 1)
