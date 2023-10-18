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
# source: spark/connect/common.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1aspark/connect/common.proto\x12\rspark.connect"\xb0\x01\n\x0cStorageLevel\x12\x19\n\x08use_disk\x18\x01 \x01(\x08R\x07useDisk\x12\x1d\n\nuse_memory\x18\x02 \x01(\x08R\tuseMemory\x12 \n\x0cuse_off_heap\x18\x03 \x01(\x08R\nuseOffHeap\x12"\n\x0c\x64\x65serialized\x18\x04 \x01(\x08R\x0c\x64\x65serialized\x12 \n\x0breplication\x18\x05 \x01(\x05R\x0breplication"G\n\x13ResourceInformation\x12\x12\n\x04name\x18\x01 \x01(\tR\x04name\x12\x1c\n\taddresses\x18\x02 \x03(\tR\taddressesB6\n\x1eorg.apache.spark.connect.protoP\x01Z\x12internal/generatedb\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "spark.connect.common_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = (
        b"\n\036org.apache.spark.connect.protoP\001Z\022internal/generated"
    )
    _STORAGELEVEL._serialized_start = 46
    _STORAGELEVEL._serialized_end = 222
    _RESOURCEINFORMATION._serialized_start = 224
    _RESOURCEINFORMATION._serialized_end = 295
# @@protoc_insertion_point(module_scope)
