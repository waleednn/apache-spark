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
    b'\n\x19spark/connect/types.proto\x12\rspark.connect"\xc8"\n\x08\x44\x61taType\x12\x32\n\x04null\x18\x01 \x01(\x0b\x32\x1c.spark.connect.DataType.NULLH\x00R\x04null\x12\x38\n\x06\x62inary\x18\x02 \x01(\x0b\x32\x1e.spark.connect.DataType.BinaryH\x00R\x06\x62inary\x12;\n\x07\x62oolean\x18\x03 \x01(\x0b\x32\x1f.spark.connect.DataType.BooleanH\x00R\x07\x62oolean\x12,\n\x02i8\x18\x04 \x01(\x0b\x32\x1a.spark.connect.DataType.I8H\x00R\x02i8\x12/\n\x03i16\x18\x05 \x01(\x0b\x32\x1b.spark.connect.DataType.I16H\x00R\x03i16\x12/\n\x03i32\x18\x06 \x01(\x0b\x32\x1b.spark.connect.DataType.I32H\x00R\x03i32\x12/\n\x03i64\x18\x07 \x01(\x0b\x32\x1b.spark.connect.DataType.I64H\x00R\x03i64\x12\x32\n\x04\x66p32\x18\x08 \x01(\x0b\x32\x1c.spark.connect.DataType.FP32H\x00R\x04\x66p32\x12\x32\n\x04\x66p64\x18\t \x01(\x0b\x32\x1c.spark.connect.DataType.FP64H\x00R\x04\x66p64\x12;\n\x07\x64\x65\x63imal\x18\n \x01(\x0b\x32\x1f.spark.connect.DataType.DecimalH\x00R\x07\x64\x65\x63imal\x12\x38\n\x06string\x18\x0b \x01(\x0b\x32\x1e.spark.connect.DataType.StringH\x00R\x06string\x12\x32\n\x04\x63har\x18\x0c \x01(\x0b\x32\x1c.spark.connect.DataType.CharH\x00R\x04\x63har\x12<\n\x08var_char\x18\r \x01(\x0b\x32\x1f.spark.connect.DataType.VarCharH\x00R\x07varChar\x12\x32\n\x04\x64\x61te\x18\x0e \x01(\x0b\x32\x1c.spark.connect.DataType.DateH\x00R\x04\x64\x61te\x12\x41\n\ttimestamp\x18\x0f \x01(\x0b\x32!.spark.connect.DataType.TimestampH\x00R\ttimestamp\x12K\n\rtimestamp_ntz\x18\x10 \x01(\x0b\x32$.spark.connect.DataType.TimestampNTZH\x00R\x0ctimestampNtz\x12W\n\x11\x63\x61lendar_interval\x18\x11 \x01(\x0b\x32(.spark.connect.DataType.CalendarIntervalH\x00R\x10\x63\x61lendarInterval\x12[\n\x13year_month_interval\x18\x12 \x01(\x0b\x32).spark.connect.DataType.YearMonthIntervalH\x00R\x11yearMonthInterval\x12U\n\x11\x64\x61y_time_interval\x18\x13 \x01(\x0b\x32\'.spark.connect.DataType.DayTimeIntervalH\x00R\x0f\x64\x61yTimeInterval\x12\x35\n\x05\x61rray\x18\x14 \x01(\x0b\x32\x1d.spark.connect.DataType.ArrayH\x00R\x05\x61rray\x12\x38\n\x06struct\x18\x15 \x01(\x0b\x32\x1e.spark.connect.DataType.StructH\x00R\x06struct\x12/\n\x03map\x18\x16 \x01(\x0b\x32\x1b.spark.connect.DataType.MapH\x00R\x03map\x12K\n\rinterval_year\x18\x17 \x01(\x0b\x32$.spark.connect.DataType.IntervalYearH\x00R\x0cintervalYear\x12H\n\x0cinterval_day\x18\x18 \x01(\x0b\x32#.spark.connect.DataType.IntervalDayH\x00R\x0bintervalDay\x12\x32\n\x04uuid\x18\x19 \x01(\x0b\x32\x1c.spark.connect.DataType.UUIDH\x00R\x04uuid\x12H\n\x0c\x66ixed_binary\x18\x1a \x01(\x0b\x32#.spark.connect.DataType.FixedBinaryH\x00R\x0b\x66ixedBinary\x12?\n\x1buser_defined_type_reference\x18\x1f \x01(\rH\x00R\x18userDefinedTypeReference\x1a\x43\n\x07\x42oolean\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a>\n\x02I8\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a?\n\x03I16\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a?\n\x03I32\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a?\n\x03I64\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04\x46P32\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04\x46P64\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x42\n\x06String\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x42\n\x06\x42inary\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04NULL\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\x45\n\tTimestamp\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a@\n\x04\x44\x61te\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aH\n\x0cTimestampNTZ\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aH\n\x0cIntervalYear\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aG\n\x0bIntervalDay\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aL\n\x10\x43\x61lendarInterval\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1a\xb3\x01\n\x11YearMonthInterval\x12$\n\x0bstart_field\x18\x01 \x01(\x05H\x00R\nstartField\x88\x01\x01\x12 \n\tend_field\x18\x02 \x01(\x05H\x01R\x08\x65ndField\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x0e\n\x0c_start_fieldB\x0c\n\n_end_field\x1a\xb1\x01\n\x0f\x44\x61yTimeInterval\x12$\n\x0bstart_field\x18\x01 \x01(\x05H\x00R\nstartField\x88\x01\x01\x12 \n\tend_field\x18\x02 \x01(\x05H\x01R\x08\x65ndField\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x0e\n\x0c_start_fieldB\x0c\n\n_end_field\x1a@\n\x04UUID\x12\x38\n\x18type_variation_reference\x18\x01 \x01(\rR\x16typeVariationReference\x1aX\n\x04\x43har\x12\x16\n\x06length\x18\x01 \x01(\x05R\x06length\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a[\n\x07VarChar\x12\x16\n\x06length\x18\x01 \x01(\x05R\x06length\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a_\n\x0b\x46ixedBinary\x12\x16\n\x06length\x18\x01 \x01(\x05R\x06length\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a\x99\x01\n\x07\x44\x65\x63imal\x12\x19\n\x05scale\x18\x01 \x01(\x05H\x00R\x05scale\x88\x01\x01\x12!\n\tprecision\x18\x02 \x01(\x05H\x01R\tprecision\x88\x01\x01\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReferenceB\x08\n\x06_scaleB\x0c\n\n_precision\x1a\xff\x01\n\x0bStructField\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x34\n\tdata_type\x18\x02 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x08\x64\x61taType\x12\x1a\n\x08nullable\x18\x03 \x01(\x08R\x08nullable\x12M\n\x08metadata\x18\x04 \x03(\x0b\x32\x31.spark.connect.DataType.StructField.MetadataEntryR\x08metadata\x1a;\n\rMetadataEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n\x05value\x18\x02 \x01(\tR\x05value:\x02\x38\x01\x1a\x7f\n\x06Struct\x12;\n\x06\x66ields\x18\x01 \x03(\x0b\x32#.spark.connect.DataType.StructFieldR\x06\x66ields\x12\x38\n\x18type_variation_reference\x18\x02 \x01(\rR\x16typeVariationReference\x1a\xa2\x01\n\x05\x41rray\x12:\n\x0c\x65lement_type\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x0b\x65lementType\x12#\n\rcontains_null\x18\x02 \x01(\x08R\x0c\x63ontainsNull\x12\x38\n\x18type_variation_reference\x18\x03 \x01(\rR\x16typeVariationReference\x1a\xdb\x01\n\x03Map\x12\x32\n\x08key_type\x18\x01 \x01(\x0b\x32\x17.spark.connect.DataTypeR\x07keyType\x12\x36\n\nvalue_type\x18\x02 \x01(\x0b\x32\x17.spark.connect.DataTypeR\tvalueType\x12.\n\x13value_contains_null\x18\x03 \x01(\x08R\x11valueContainsNull\x12\x38\n\x18type_variation_reference\x18\x04 \x01(\rR\x16typeVariationReferenceB\x06\n\x04kindB"\n\x1eorg.apache.spark.connect.protoP\x01\x62\x06proto3'
)


_DATATYPE = DESCRIPTOR.message_types_by_name["DataType"]
_DATATYPE_BOOLEAN = _DATATYPE.nested_types_by_name["Boolean"]
_DATATYPE_I8 = _DATATYPE.nested_types_by_name["I8"]
_DATATYPE_I16 = _DATATYPE.nested_types_by_name["I16"]
_DATATYPE_I32 = _DATATYPE.nested_types_by_name["I32"]
_DATATYPE_I64 = _DATATYPE.nested_types_by_name["I64"]
_DATATYPE_FP32 = _DATATYPE.nested_types_by_name["FP32"]
_DATATYPE_FP64 = _DATATYPE.nested_types_by_name["FP64"]
_DATATYPE_STRING = _DATATYPE.nested_types_by_name["String"]
_DATATYPE_BINARY = _DATATYPE.nested_types_by_name["Binary"]
_DATATYPE_NULL = _DATATYPE.nested_types_by_name["NULL"]
_DATATYPE_TIMESTAMP = _DATATYPE.nested_types_by_name["Timestamp"]
_DATATYPE_DATE = _DATATYPE.nested_types_by_name["Date"]
_DATATYPE_TIMESTAMPNTZ = _DATATYPE.nested_types_by_name["TimestampNTZ"]
_DATATYPE_INTERVALYEAR = _DATATYPE.nested_types_by_name["IntervalYear"]
_DATATYPE_INTERVALDAY = _DATATYPE.nested_types_by_name["IntervalDay"]
_DATATYPE_CALENDARINTERVAL = _DATATYPE.nested_types_by_name["CalendarInterval"]
_DATATYPE_YEARMONTHINTERVAL = _DATATYPE.nested_types_by_name["YearMonthInterval"]
_DATATYPE_DAYTIMEINTERVAL = _DATATYPE.nested_types_by_name["DayTimeInterval"]
_DATATYPE_UUID = _DATATYPE.nested_types_by_name["UUID"]
_DATATYPE_CHAR = _DATATYPE.nested_types_by_name["Char"]
_DATATYPE_VARCHAR = _DATATYPE.nested_types_by_name["VarChar"]
_DATATYPE_FIXEDBINARY = _DATATYPE.nested_types_by_name["FixedBinary"]
_DATATYPE_DECIMAL = _DATATYPE.nested_types_by_name["Decimal"]
_DATATYPE_STRUCTFIELD = _DATATYPE.nested_types_by_name["StructField"]
_DATATYPE_STRUCTFIELD_METADATAENTRY = _DATATYPE_STRUCTFIELD.nested_types_by_name["MetadataEntry"]
_DATATYPE_STRUCT = _DATATYPE.nested_types_by_name["Struct"]
_DATATYPE_ARRAY = _DATATYPE.nested_types_by_name["Array"]
_DATATYPE_MAP = _DATATYPE.nested_types_by_name["Map"]
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
        "I8": _reflection.GeneratedProtocolMessageType(
            "I8",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_I8,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.I8)
            },
        ),
        "I16": _reflection.GeneratedProtocolMessageType(
            "I16",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_I16,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.I16)
            },
        ),
        "I32": _reflection.GeneratedProtocolMessageType(
            "I32",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_I32,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.I32)
            },
        ),
        "I64": _reflection.GeneratedProtocolMessageType(
            "I64",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_I64,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.I64)
            },
        ),
        "FP32": _reflection.GeneratedProtocolMessageType(
            "FP32",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_FP32,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.FP32)
            },
        ),
        "FP64": _reflection.GeneratedProtocolMessageType(
            "FP64",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_FP64,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.FP64)
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
        "IntervalYear": _reflection.GeneratedProtocolMessageType(
            "IntervalYear",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_INTERVALYEAR,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.IntervalYear)
            },
        ),
        "IntervalDay": _reflection.GeneratedProtocolMessageType(
            "IntervalDay",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_INTERVALDAY,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.IntervalDay)
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
        "UUID": _reflection.GeneratedProtocolMessageType(
            "UUID",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_UUID,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.UUID)
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
        "FixedBinary": _reflection.GeneratedProtocolMessageType(
            "FixedBinary",
            (_message.Message,),
            {
                "DESCRIPTOR": _DATATYPE_FIXEDBINARY,
                "__module__": "spark.connect.types_pb2"
                # @@protoc_insertion_point(class_scope:spark.connect.DataType.FixedBinary)
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
                "MetadataEntry": _reflection.GeneratedProtocolMessageType(
                    "MetadataEntry",
                    (_message.Message,),
                    {
                        "DESCRIPTOR": _DATATYPE_STRUCTFIELD_METADATAENTRY,
                        "__module__": "spark.connect.types_pb2"
                        # @@protoc_insertion_point(class_scope:spark.connect.DataType.StructField.MetadataEntry)
                    },
                ),
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
        "DESCRIPTOR": _DATATYPE,
        "__module__": "spark.connect.types_pb2"
        # @@protoc_insertion_point(class_scope:spark.connect.DataType)
    },
)
_sym_db.RegisterMessage(DataType)
_sym_db.RegisterMessage(DataType.Boolean)
_sym_db.RegisterMessage(DataType.I8)
_sym_db.RegisterMessage(DataType.I16)
_sym_db.RegisterMessage(DataType.I32)
_sym_db.RegisterMessage(DataType.I64)
_sym_db.RegisterMessage(DataType.FP32)
_sym_db.RegisterMessage(DataType.FP64)
_sym_db.RegisterMessage(DataType.String)
_sym_db.RegisterMessage(DataType.Binary)
_sym_db.RegisterMessage(DataType.NULL)
_sym_db.RegisterMessage(DataType.Timestamp)
_sym_db.RegisterMessage(DataType.Date)
_sym_db.RegisterMessage(DataType.TimestampNTZ)
_sym_db.RegisterMessage(DataType.IntervalYear)
_sym_db.RegisterMessage(DataType.IntervalDay)
_sym_db.RegisterMessage(DataType.CalendarInterval)
_sym_db.RegisterMessage(DataType.YearMonthInterval)
_sym_db.RegisterMessage(DataType.DayTimeInterval)
_sym_db.RegisterMessage(DataType.UUID)
_sym_db.RegisterMessage(DataType.Char)
_sym_db.RegisterMessage(DataType.VarChar)
_sym_db.RegisterMessage(DataType.FixedBinary)
_sym_db.RegisterMessage(DataType.Decimal)
_sym_db.RegisterMessage(DataType.StructField)
_sym_db.RegisterMessage(DataType.StructField.MetadataEntry)
_sym_db.RegisterMessage(DataType.Struct)
_sym_db.RegisterMessage(DataType.Array)
_sym_db.RegisterMessage(DataType.Map)

if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\036org.apache.spark.connect.protoP\001"
    _DATATYPE_STRUCTFIELD_METADATAENTRY._options = None
    _DATATYPE_STRUCTFIELD_METADATAENTRY._serialized_options = b"8\001"
    _DATATYPE._serialized_start = 45
    _DATATYPE._serialized_end = 4469
    _DATATYPE_BOOLEAN._serialized_start = 1727
    _DATATYPE_BOOLEAN._serialized_end = 1794
    _DATATYPE_I8._serialized_start = 1796
    _DATATYPE_I8._serialized_end = 1858
    _DATATYPE_I16._serialized_start = 1860
    _DATATYPE_I16._serialized_end = 1923
    _DATATYPE_I32._serialized_start = 1925
    _DATATYPE_I32._serialized_end = 1988
    _DATATYPE_I64._serialized_start = 1990
    _DATATYPE_I64._serialized_end = 2053
    _DATATYPE_FP32._serialized_start = 2055
    _DATATYPE_FP32._serialized_end = 2119
    _DATATYPE_FP64._serialized_start = 2121
    _DATATYPE_FP64._serialized_end = 2185
    _DATATYPE_STRING._serialized_start = 2187
    _DATATYPE_STRING._serialized_end = 2253
    _DATATYPE_BINARY._serialized_start = 2255
    _DATATYPE_BINARY._serialized_end = 2321
    _DATATYPE_NULL._serialized_start = 2323
    _DATATYPE_NULL._serialized_end = 2387
    _DATATYPE_TIMESTAMP._serialized_start = 2389
    _DATATYPE_TIMESTAMP._serialized_end = 2458
    _DATATYPE_DATE._serialized_start = 2460
    _DATATYPE_DATE._serialized_end = 2524
    _DATATYPE_TIMESTAMPNTZ._serialized_start = 2526
    _DATATYPE_TIMESTAMPNTZ._serialized_end = 2598
    _DATATYPE_INTERVALYEAR._serialized_start = 2600
    _DATATYPE_INTERVALYEAR._serialized_end = 2672
    _DATATYPE_INTERVALDAY._serialized_start = 2674
    _DATATYPE_INTERVALDAY._serialized_end = 2745
    _DATATYPE_CALENDARINTERVAL._serialized_start = 2747
    _DATATYPE_CALENDARINTERVAL._serialized_end = 2823
    _DATATYPE_YEARMONTHINTERVAL._serialized_start = 2826
    _DATATYPE_YEARMONTHINTERVAL._serialized_end = 3005
    _DATATYPE_DAYTIMEINTERVAL._serialized_start = 3008
    _DATATYPE_DAYTIMEINTERVAL._serialized_end = 3185
    _DATATYPE_UUID._serialized_start = 3187
    _DATATYPE_UUID._serialized_end = 3251
    _DATATYPE_CHAR._serialized_start = 3253
    _DATATYPE_CHAR._serialized_end = 3341
    _DATATYPE_VARCHAR._serialized_start = 3343
    _DATATYPE_VARCHAR._serialized_end = 3434
    _DATATYPE_FIXEDBINARY._serialized_start = 3436
    _DATATYPE_FIXEDBINARY._serialized_end = 3531
    _DATATYPE_DECIMAL._serialized_start = 3534
    _DATATYPE_DECIMAL._serialized_end = 3687
    _DATATYPE_STRUCTFIELD._serialized_start = 3690
    _DATATYPE_STRUCTFIELD._serialized_end = 3945
    _DATATYPE_STRUCTFIELD_METADATAENTRY._serialized_start = 3886
    _DATATYPE_STRUCTFIELD_METADATAENTRY._serialized_end = 3945
    _DATATYPE_STRUCT._serialized_start = 3947
    _DATATYPE_STRUCT._serialized_end = 4074
    _DATATYPE_ARRAY._serialized_start = 4077
    _DATATYPE_ARRAY._serialized_end = 4239
    _DATATYPE_MAP._serialized_start = 4242
    _DATATYPE_MAP._serialized_end = 4461
# @@protoc_insertion_point(module_scope)
