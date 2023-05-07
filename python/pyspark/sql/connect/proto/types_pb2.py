#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: spark/connect/types.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b"\n\x19spark/connect/types.proto\x12\rspark.connect\"\xc7 \n\x08\x44\x61taType\x12\x32\n\x04null\x18\x01 \x01(\x0b\x32\x1c.spark.connect.DataType.NULLH\x00R\x04null\x12\x38\n\x06\x62inary\x18\x02 \x01(\x0b\x32\x1e.spark.connect.DataType.BinaryH\x00R\x06\x62inary\x12;\n\x07\x62oolean\x18\x03 \x01(\x0b\x32\x1f.spark.connect.DataType.BooleanH\x00R\x07\x62oolean\x12\x32\n\x04\x62yte\x18\x04 \x01(\x0b\x32\x1c.spark.connect.DataType.ByteH\x00R\x04\x62yte\x12\x35\n\x05short\x18\x05 \x01(\x0b\x32\x1d.spark.connect.DataType.ShortH\x00R\x05short\x12;\n\x07integer\x18\x06 \x01(\x0b\x32\x1f.spark.connect.DataType.IntegerH\x00R\x07integer\x12\x32\n\x04long\x18\x07 \x01(\x0b\x32\x1c.spark.connect.DataType.LongH\x00R\x04long\x12\x35\n\x05\x66loat\x18\x08 \x01(\x0b\x32\x1d.spark.connect.DataType.FloatH\x00R\x05\x66loat\x12\x38\n\x06\x64ouble\x18\t \x01(\x0b\x32\x1e.spark.connect.DataType.DoubleH\x00R\x06\x64ouble\x12;\n\x07\x64\x65\x63imal\x18\n \x01(\x0b\x32\x1f.spark.connect.DataType.DecimalH\x00R\x07\x64\x65\x63imal\x12\x38\n\x06string\x18\x0b \x01(\x0b\x32\x1e.spark.connect.DataType.StringH\x00R\x06string\x12\x32\n\x04\x63har\x18\x0c \x01(\x0b\x32\x1c.spark.connect.DataType.CharH\x00R\x04\x63har\x12<\n\x08var_char\x18\r \x01(\x0b\x32\x1f.spark.connect.DataType.VarCharH\x00R\x07varChar\x12\x32\n\x04\x64\x61te\x18\x0e \x01(\x0b\x32\x1c.spark.connect.DataType.DateH\x00R\x04\x64\x61te\x12\x41\n\ttimestamp\x18\x0f \x01(\x0b\x32!.spark.connect.DataType.TimestampH\x00R\ttimestamp\x12K\n\rtimestamp_ntz\x18\x10 \x01(\x0b\x32$.spark.connect.DataType.TimestampNTZH\x00R\x0ctimestampNtz\x12W\n\x11\x63\x61lendar_interval\x18\x11 \x01(\x0b\x32(.spark.connect.DataType.CalendarIntervalH\x00R\x10\x63\x61lendarInterval\x12[\n\x13year_month_interval\x18\x12 \x01(\x0b\x32).spark.connect.DataType.YearMonthIntervalH\x00R\x11yearMonthInterval\x12U\n\x11\x64\x61y_time_interval\x18\x13 \x01(\x0b\x32'.spark.connect.DataType.DayTimeIntervalH\x00R\x0f\x64\x61yTimeInterval\x12\x35\n\x05\x61rray\x18\x14 \x01(\x0b\x32\x1d.spark.connect.DataType.ArrayH\x00R\x05\x61rray\x12\x38\n\x06struct\x18\x15 \x01(\x0b\x32\x1e.spark.connect.DataType.StructH\x00R\x06struct\x12/\n\x03map\x18\x16 \x01(\x0b\x32\x1b.spark.connect.DataType.MapH\x00R\x03map\x12/\n\x03udt\x18\x17 \x01(\x0b\x32\x1b.spark.connect.DataType.UDTH\x00R\x03udt\x12>\n\x08unparsed\x18\x18 \x01(\x0b\x32 .spark.connect.DataType.UnparsedH\x00R\x08unparsed\x1a\x43\n\x07\x42oolean\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04\x42yte\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x41\n\x05Short\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x43\n\x07Integer\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04Long\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x41\n\x05\x46loat\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x42\n\x06\x44ouble\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x42\n\x06String\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x42\n\x06\x42inary\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04NULL\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x45\n\tTimestamp\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04\x44\x61te\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aH\n\x0cTimestampNTZ\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aL\n\x10\x43\x61lendarInterval\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\xb3\x01\n\x11YearMonthInterval\x12$\n\x0bstart_field\x18\x01 \x01(\x05H\x00R\nstartField\x88\x01\x01\x12 \n\tend_field\x18\x02 \x01(\x05H\x01R\x08\x65ndField\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x0e\n\x0c_start_fieldB\x0c\n\n_end_field\x1a\xb1\x01\n\x0f\x44\x61yTimeInterval\x12$\n\x0bstart_field\x18\x01 \x01(\x05H\x00R\nstartField\x88\x01\x01\x12 \n\tend_field\x18\x02 \x01(\x05H\x01R\x08\x65ndField\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x0e\n\x0c_start_fieldB\x0c\n\n_end_field\x1aX\n\x04\x43har\x12\x16\n\x06length\x18\x01 \x01(\x05R\x06length\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a[\n\x07VarChar\x12\x16\n\x06length\x18\x01 \x01(\x05R\x06length\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a\x99\x01\n\x07\x44\x65\x63imal\x12\x19\n\x05scale\x18\x01 \x01(\x05H\x00R\x05scale\x88\x01\x01\x12!\n\tprecision\x18\x02 \x01(\x05H\x01R\tprecision\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x08\n\x06_scaleB\x0c\n\n_precision\x1a\xa1\x01\n\x0bStructField\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x34\n\tdata_type\x18\x02 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x08\x64\x61taType\x12\x1a\n\x08nullable\x18\x03 \x01(\x08R\x08nullable\x12\x1f\n\x08metadata\x18\x04 \x01(\tH\x00R\x08metadata\x88\x01\x01\x42\x0b\n\t_metadata\x1a\x7f\n\x06Struct\x12;\n\x06\x66ields\x18\x01 \x03(\x0b\x32#.spark.connect.DataType.StructFieldR\x06\x66ields\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a\xa2\x01\n\x05\x41rray\x12:\n\x0c\x65lement_type\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x0b\x65lementType\x12#\n\rcontains_null\x18\x02 \x01(\x08R\x0c\x63ontainsNull\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReference\x1a\xdb\x01\n\x03Map\x12\x32\n\x08key_type\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x07keyType\x12\x36\n\nvalue_type\x18\x02 \x01(\x0b\x32\x17.spark.connect.DataTypeR\tvalueType\x12.\n\x13value_contains_null\x18\x03 \x01(\x08R\x11valueContainsNull\x12\x38\n\x18type_variation_reference\x18\x04 \x01(\rR\x16typeVariationReference\x1a\x8f\x02\n\x03UDT\x12\x12\n\x04type\x18\x01 \x01(\tR\x04type\x12 \n\tjvm_class\x18\x02 \x01(\tH\x00R\x08jvmClass\x88\x01\x01\x12&\n\x0cpython_class\x18\x03 \x01(\tH\x01R\x0bpythonClass\x88\x01\x01\x12;\n\x17serialized_python_class\x18\x04 \x01(\tH\x02R\x15serializedPythonClass\x88\x01\x01\x12\x32\n\x08sql_type\x18\x05 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x07sqlTypeB\x0c\n\n_jvm_classB\x0f\n\r_python_classB\x1a\n\x18_serialized_python_class\x1a\x34\n\x08Unparsed\x12(\n\x10\x64\x61ta_type_string\x18\x01 \x01(\tR\x0e\x64\x61taTypeStringB\x06\n\x04kindBW\n\x1eorg.apache.spark.connect.protoP\x01Z3github.com/apache/spark/go/v_3_4/internal/generatedb\x06proto3"
)


_DATATYPE = DESCRIPTOR.message_types_by_name["DataType"]
_DATATYPE_BOOLEAN = _DATATYPE.nested_types_by_name["Boolean"]
_DATATYPE_BYTE = _DATATYPE.nested_types_by_name["Byte"]
_DATATYPE_SHORT = _DATATYPE.nested_types_by_name["Short"]
_DATATYPE_INTEGER = _DATATYPE.nested_types_by_name["Integer"]
_DATATYPE_LONG = _DATATYPE.nested_types_by_name["Long"]
_DATATYPE_FLOAT = _DATATYPE.nested_types_by_name["Float"]
_DATATYPE_DOUBLE = _DATATYPE.nested_types_by_name["Double"]
_DATATYPE_STRING = _DATATYPE.nested_types_by_name["String"]
_DATATYPE_BINARY = _DATATYPE.nested_types_by_name["Binary"]
_DATATYPE_NULL = _DATATYPE.nested_types_by_name["NULL"]
_DATATYPE_TIMESTAMP = _DATATYPE.nested_types_by_name["Timestamp"]
_DATATYPE_DATE = _DATATYPE.nested_types_by_name["Date"]
_DATATYPE_TIMESTAMPNTZ = _DATATYPE.nested_types_by_name["TimestampNTZ"]
_DATATYPE_CALENDARINTERVAL = _DATATYPE.nested_types_by_name["CalendarInterval"]
_DATATYPE_YEARMONTHINTERVAL = _DATATYPE.nested_types_by_name["YearMonthInterval"]
_DATATYPE_DAYTIMEINTERVAL = _DATATYPE.nested_types_by_name["DayTimeInterval"]
_DATATYPE_CHAR = _DATATYPE.nested_types_by_name["Char"]
_DATATYPE_VARCHAR = _DATATYPE.nested_types_by_name["VarChar"]
_DATATYPE_DECIMAL = _DATATYPE.nested_types_by_name["Decimal"]
_DATATYPE_STRUCTFIELD = _DATATYPE.nested_types_by_name["StructField"]
_DATATYPE_STRUCT = _DATATYPE.nested_types_by_name["Struct"]
_DATATYPE_ARRAY = _DATATYPE.nested_types_by_name["Array"]
_DATATYPE_MAP = _DATATYPE.nested_types_by_name["Map"]
_DATATYPE_UDT = _DATATYPE.nested_types_by_name["UDT"]
_DATATYPE_UNPARSED = _DATATYPE.nested_types_by_name["Unparsed"]
DataType = _reflection.GeneratedProtocolMessageType(
    "DataType",
    (_message.Message,),
    {
        "Boolean": _reflection.GeneratedProtocolMessageType(
            "Boolean",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_BOOLEAN,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Boolean)
            },
        ),
        "Byte": _reflection.GeneratedProtocolMessageType(
            "Byte",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_BYTE,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Byte)
            },
        ),
        "Short": _reflection.GeneratedProtocolMessageType(
            "Short",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_SHORT,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Short)
            },
        ),
        "Integer": _reflection.GeneratedProtocolMessageType(
            "Integer",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_INTEGER,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Integer)
            },
        ),
        "Long": _reflection.GeneratedProtocolMessageType(
            "Long",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_LONG,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Long)
            },
        ),
        "Float": _reflection.GeneratedProtocolMessageType(
            "Float",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_FLOAT,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Float)
            },
        ),
        "Double": _reflection.GeneratedProtocolMessageType(
            "Double",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_DOUBLE,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Double)
            },
        ),
        "String": _reflection.GeneratedProtocolMessageType(
            "String",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_STRING,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.String)
            },
        ),
        "Binary": _reflection.GeneratedProtocolMessageType(
            "Binary",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_BINARY,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Binary)
            },
        ),
        "NULL": _reflection.GeneratedProtocolMessageType(
            "NULL",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_NULL,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.NULL)
            },
        ),
        "Timestamp": _reflection.GeneratedProtocolMessageType(
            "Timestamp",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_TIMESTAMP,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Timestamp)
            },
        ),
        "Date": _reflection.GeneratedProtocolMessageType(
            "Date",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_DATE,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Date)
            },
        ),
        "TimestampNTZ": _reflection.GeneratedProtocolMessageType(
            "TimestampNTZ",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_TIMESTAMPNTZ,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.TimestampNTZ)
            },
        ),
        "CalendarInterval": _reflection.GeneratedProtocolMessageType(
            "CalendarInterval",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_CALENDARINTERVAL,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.CalendarInterval)
            },
        ),
        "YearMonthInterval": _reflection.GeneratedProtocolMessageType(
            "YearMonthInterval",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_YEARMONTHINTERVAL,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.YearMonthInterval)
            },
        ),
        "DayTimeInterval": _reflection.GeneratedProtocolMessageType(
            "DayTimeInterval",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_DAYTIMEINTERVAL,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.DayTimeInterval)
            },
        ),
        "Char": _reflection.GeneratedProtocolMessageType(
            "Char",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_CHAR,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Char)
            },
        ),
        "VarChar": _reflection.GeneratedProtocolMessageType(
            "VarChar",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_VARCHAR,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.VarChar)
            },
        ),
        "Decimal": _reflection.GeneratedProtocolMessageType(
            "Decimal",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_DECIMAL,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Decimal)
            },
        ),
        "StructField": _reflection.GeneratedProtocolMessageType(
            "StructField",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_STRUCTFIELD,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.StructField)
            },
        ),
        "Struct": _reflection.GeneratedProtocolMessageType(
            "Struct",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_STRUCT,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Struct)
            },
        ),
        "Array": _reflection.GeneratedProtocolMessageType(
            "Array",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_ARRAY,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Array)
            },
        ),
        "Map": _reflection.GeneratedProtocolMessageType(
            "Map",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_MAP,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Map)
            },
        ),
        "UDT": _reflection.GeneratedProtocolMessageType(
            "UDT",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_UDT,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.UDT)
            },
        ),
        "Unparsed": _reflection.GeneratedProtocolMessageType(
            "Unparsed",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_UNPARSED,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.Unparsed)
            },
        ),
        "DESCRIPTOR": _DATATYPE,
        "__module__": "spark.connect.types_pb2"
        # @@protoc_insertion_point(class_scope:spark.connect.DataType)
    },
)
_sym_db.RegisterMessage(DataType)
_sym_db.RegisterMessage(DataType.Boolean)
_sym_db.RegisterMessage(DataType.Byte)
_sym_db.RegisterMessage(DataType.Short)
_sym_db.RegisterMessage(DataType.Integer)
_sym_db.RegisterMessage(DataType.Long)
_sym_db.RegisterMessage(DataType.Float)
_sym_db.RegisterMessage(DataType.Double)
_sym_db.RegisterMessage(DataType.String)
_sym_db.RegisterMessage(DataType.Binary)
_sym_db.RegisterMessage(DataType.NULL)
_sym_db.RegisterMessage(DataType.Timestamp)
_sym_db.RegisterMessage(DataType.Date)
_sym_db.RegisterMessage(DataType.TimestampNTZ)
_sym_db.RegisterMessage(DataType.CalendarInterval)
_sym_db.RegisterMessage(DataType.YearMonthInterval)
_sym_db.RegisterMessage(DataType.DayTimeInterval)
_sym_db.RegisterMessage(DataType.Char)
_sym_db.RegisterMessage(DataType.VarChar)
_sym_db.RegisterMessage(DataType.Decimal)
_sym_db.RegisterMessage(DataType.StructField)
_sym_db.RegisterMessage(DataType.Struct)
_sym_db.RegisterMessage(DataType.Array)
_sym_db.RegisterMessage(DataType.Map)
_sym_db.RegisterMessage(DataType.UDT)
_sym_db.RegisterMessage(DataType.Unparsed)

if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\036org.apache.spark.connect.protoP\001Z3github.com/apache/spark/go/v_3_4/internal/generated"
    _DATATYPE._serialized_start = 45
    _DATATYPE._serialized_end = 4212
    _DATATYPE_BOOLEAN._serialized_start = 1534
    _DATATYPE_BOOLEAN._serialized_end = 1601
    _DATATYPE_BYTE._serialized_start = 1603
    _DATATYPE_BYTE._serialized_end = 1667
    _DATATYPE_SHORT._serialized_start = 1669
    _DATATYPE_SHORT._serialized_end = 1734
    _DATATYPE_INTEGER._serialized_start = 1736
    _DATATYPE_INTEGER._serialized_end = 1803
    _DATATYPE_LONG._serialized_start = 1805
    _DATATYPE_LONG._serialized_end = 1869
    _DATATYPE_FLOAT._serialized_start = 1871
    _DATATYPE_FLOAT._serialized_end = 1936
    _DATATYPE_DOUBLE._serialized_start = 1938
    _DATATYPE_DOUBLE._serialized_end = 2004
    _DATATYPE_STRING._serialized_start = 2006
    _DATATYPE_STRING._serialized_end = 2072
    _DATATYPE_BINARY._serialized_start = 2074
    _DATATYPE_BINARY._serialized_end = 2140
    _DATATYPE_NULL._serialized_start = 2142
    _DATATYPE_NULL._serialized_end = 2206
    _DATATYPE_TIMESTAMP._serialized_start = 2208
    _DATATYPE_TIMESTAMP._serialized_end = 2277
    _DATATYPE_DATE._serialized_start = 2279
    _DATATYPE_DATE._serialized_end = 2343
    _DATATYPE_TIMESTAMPNTZ._serialized_start = 2345
    _DATATYPE_TIMESTAMPNTZ._serialized_end = 2417
    _DATATYPE_CALENDARINTERVAL._serialized_start = 2419
    _DATATYPE_CALENDARINTERVAL._serialized_end = 2495
    _DATATYPE_YEARMONTHINTERVAL._serialized_start = 2498
    _DATATYPE_YEARMONTHINTERVAL._serialized_end = 2677
    _DATATYPE_DAYTIMEINTERVAL._serialized_start = 2680
    _DATATYPE_DAYTIMEINTERVAL._serialized_end = 2857
    _DATATYPE_CHAR._serialized_start = 2859
    _DATATYPE_CHAR._serialized_end = 2947
    _DATATYPE_VARCHAR._serialized_start = 2949
    _DATATYPE_VARCHAR._serialized_end = 3040
    _DATATYPE_DECIMAL._serialized_start = 3043
    _DATATYPE_DECIMAL._serialized_end = 3196
    _DATATYPE_STRUCTFIELD._serialized_start = 3199
    _DATATYPE_STRUCTFIELD._serialized_end = 3360
    _DATATYPE_STRUCT._serialized_start = 3362
    _DATATYPE_STRUCT._serialized_end = 3489
    _DATATYPE_ARRAY._serialized_start = 3492
    _DATATYPE_ARRAY._serialized_end = 3654
    _DATATYPE_MAP._serialized_start = 3657
    _DATATYPE_MAP._serialized_end = 3876
    _DATATYPE_UDT._serialized_start = 3879
    _DATATYPE_UDT._serialized_end = 4150
    _DATATYPE_UNPARSED._serialized_start = 4152
    _DATATYPE_UNPARSED._serialized_end = 4204
# @@protoc_insertion_point(module_scope)
