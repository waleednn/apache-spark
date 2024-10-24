# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: StateMessage.proto
# Protobuf Python Version: 5.27.3
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x12StateMessage.proto\x12.org.apache.spark.sql.execution.streaming.state"\xe9\x02\n\x0cStateRequest\x12\x0f\n\x07version\x18\x01 \x01(\x05\x12\x66\n\x15statefulProcessorCall\x18\x02 \x01(\x0b\x32\x45.org.apache.spark.sql.execution.streaming.state.StatefulProcessorCallH\x00\x12\x64\n\x14stateVariableRequest\x18\x03 \x01(\x0b\x32\x44.org.apache.spark.sql.execution.streaming.state.StateVariableRequestH\x00\x12p\n\x1aimplicitGroupingKeyRequest\x18\x04 \x01(\x0b\x32J.org.apache.spark.sql.execution.streaming.state.ImplicitGroupingKeyRequestH\x00\x42\x08\n\x06method"H\n\rStateResponse\x12\x12\n\nstatusCode\x18\x01 \x01(\x05\x12\x14\n\x0c\x65rrorMessage\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\x0c"\xe5\x03\n\x15StatefulProcessorCall\x12X\n\x0esetHandleState\x18\x01 \x01(\x0b\x32>.org.apache.spark.sql.execution.streaming.state.SetHandleStateH\x00\x12Y\n\rgetValueState\x18\x02 \x01(\x0b\x32@.org.apache.spark.sql.execution.streaming.state.StateCallCommandH\x00\x12X\n\x0cgetListState\x18\x03 \x01(\x0b\x32@.org.apache.spark.sql.execution.streaming.state.StateCallCommandH\x00\x12W\n\x0bgetMapState\x18\x04 \x01(\x0b\x32@.org.apache.spark.sql.execution.streaming.state.StateCallCommandH\x00\x12Z\n\x0e\x64\x65leteIfExists\x18\x05 \x01(\x0b\x32@.org.apache.spark.sql.execution.streaming.state.StateCallCommandH\x00\x42\x08\n\x06method"\xa8\x02\n\x14StateVariableRequest\x12X\n\x0evalueStateCall\x18\x01 \x01(\x0b\x32>.org.apache.spark.sql.execution.streaming.state.ValueStateCallH\x00\x12V\n\rlistStateCall\x18\x02 \x01(\x0b\x32=.org.apache.spark.sql.execution.streaming.state.ListStateCallH\x00\x12T\n\x0cmapStateCall\x18\x03 \x01(\x0b\x32<.org.apache.spark.sql.execution.streaming.state.MapStateCallH\x00\x42\x08\n\x06method"\xe0\x01\n\x1aImplicitGroupingKeyRequest\x12X\n\x0esetImplicitKey\x18\x01 \x01(\x0b\x32>.org.apache.spark.sql.execution.streaming.state.SetImplicitKeyH\x00\x12^\n\x11removeImplicitKey\x18\x02 \x01(\x0b\x32\x41.org.apache.spark.sql.execution.streaming.state.RemoveImplicitKeyH\x00\x42\x08\n\x06method"\x9a\x01\n\x10StateCallCommand\x12\x11\n\tstateName\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12\x1b\n\x13mapStateValueSchema\x18\x03 \x01(\t\x12\x46\n\x03ttl\x18\x04 \x01(\x0b\x32\x39.org.apache.spark.sql.execution.streaming.state.TTLConfig"\xe1\x02\n\x0eValueStateCall\x12\x11\n\tstateName\x18\x01 \x01(\t\x12H\n\x06\x65xists\x18\x02 \x01(\x0b\x32\x36.org.apache.spark.sql.execution.streaming.state.ExistsH\x00\x12\x42\n\x03get\x18\x03 \x01(\x0b\x32\x33.org.apache.spark.sql.execution.streaming.state.GetH\x00\x12\\\n\x10valueStateUpdate\x18\x04 \x01(\x0b\x32@.org.apache.spark.sql.execution.streaming.state.ValueStateUpdateH\x00\x12\x46\n\x05\x63lear\x18\x05 \x01(\x0b\x32\x35.org.apache.spark.sql.execution.streaming.state.ClearH\x00\x42\x08\n\x06method"\x90\x04\n\rListStateCall\x12\x11\n\tstateName\x18\x01 \x01(\t\x12H\n\x06\x65xists\x18\x02 \x01(\x0b\x32\x36.org.apache.spark.sql.execution.streaming.state.ExistsH\x00\x12T\n\x0clistStateGet\x18\x03 \x01(\x0b\x32<.org.apache.spark.sql.execution.streaming.state.ListStateGetH\x00\x12T\n\x0clistStatePut\x18\x04 \x01(\x0b\x32<.org.apache.spark.sql.execution.streaming.state.ListStatePutH\x00\x12R\n\x0b\x61ppendValue\x18\x05 \x01(\x0b\x32;.org.apache.spark.sql.execution.streaming.state.AppendValueH\x00\x12P\n\nappendList\x18\x06 \x01(\x0b\x32:.org.apache.spark.sql.execution.streaming.state.AppendListH\x00\x12\x46\n\x05\x63lear\x18\x07 \x01(\x0b\x32\x35.org.apache.spark.sql.execution.streaming.state.ClearH\x00\x42\x08\n\x06method"\xe1\x05\n\x0cMapStateCall\x12\x11\n\tstateName\x18\x01 \x01(\t\x12H\n\x06\x65xists\x18\x02 \x01(\x0b\x32\x36.org.apache.spark.sql.execution.streaming.state.ExistsH\x00\x12L\n\x08getValue\x18\x03 \x01(\x0b\x32\x38.org.apache.spark.sql.execution.streaming.state.GetValueH\x00\x12R\n\x0b\x63ontainsKey\x18\x04 \x01(\x0b\x32;.org.apache.spark.sql.execution.streaming.state.ContainsKeyH\x00\x12R\n\x0bupdateValue\x18\x05 \x01(\x0b\x32;.org.apache.spark.sql.execution.streaming.state.UpdateValueH\x00\x12L\n\x08iterator\x18\x06 \x01(\x0b\x32\x38.org.apache.spark.sql.execution.streaming.state.IteratorH\x00\x12\x44\n\x04keys\x18\x07 \x01(\x0b\x32\x34.org.apache.spark.sql.execution.streaming.state.KeysH\x00\x12H\n\x06values\x18\x08 \x01(\x0b\x32\x36.org.apache.spark.sql.execution.streaming.state.ValuesH\x00\x12N\n\tremoveKey\x18\t \x01(\x0b\x32\x39.org.apache.spark.sql.execution.streaming.state.RemoveKeyH\x00\x12\x46\n\x05\x63lear\x18\n \x01(\x0b\x32\x35.org.apache.spark.sql.execution.streaming.state.ClearH\x00\x42\x08\n\x06method"\x1d\n\x0eSetImplicitKey\x12\x0b\n\x03key\x18\x01 \x01(\x0c"\x13\n\x11RemoveImplicitKey"\x08\n\x06\x45xists"\x05\n\x03Get"!\n\x10ValueStateUpdate\x12\r\n\x05value\x18\x01 \x01(\x0c"\x07\n\x05\x43lear""\n\x0cListStateGet\x12\x12\n\niteratorId\x18\x01 \x01(\t"\x0e\n\x0cListStatePut"\x1c\n\x0b\x41ppendValue\x12\r\n\x05value\x18\x01 \x01(\x0c"\x0c\n\nAppendList"\x1b\n\x08GetValue\x12\x0f\n\x07userKey\x18\x01 \x01(\x0c"\x1e\n\x0b\x43ontainsKey\x12\x0f\n\x07userKey\x18\x01 \x01(\x0c"-\n\x0bUpdateValue\x12\x0f\n\x07userKey\x18\x01 \x01(\x0c\x12\r\n\x05value\x18\x02 \x01(\x0c"\x1e\n\x08Iterator\x12\x12\n\niteratorId\x18\x01 \x01(\t"\x1a\n\x04Keys\x12\x12\n\niteratorId\x18\x01 \x01(\t"\x1c\n\x06Values\x12\x12\n\niteratorId\x18\x01 \x01(\t"\x1c\n\tRemoveKey\x12\x0f\n\x07userKey\x18\x01 \x01(\x0c"\\\n\x0eSetHandleState\x12J\n\x05state\x18\x01 \x01(\x0e\x32;.org.apache.spark.sql.execution.streaming.state.HandleState"\x1f\n\tTTLConfig\x12\x12\n\ndurationMs\x18\x01 \x01(\x05*K\n\x0bHandleState\x12\x0b\n\x07\x43REATED\x10\x00\x12\x0f\n\x0bINITIALIZED\x10\x01\x12\x12\n\x0e\x44\x41TA_PROCESSED\x10\x02\x12\n\n\x06\x43LOSED\x10\x03\x62\x06proto3'  # noqa: E501
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "StateMessage_pb2", _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals["_HANDLESTATE"]._serialized_start = 3870
    _globals["_HANDLESTATE"]._serialized_end = 3945
    _globals["_STATEREQUEST"]._serialized_start = 71
    _globals["_STATEREQUEST"]._serialized_end = 432
    _globals["_STATERESPONSE"]._serialized_start = 434
    _globals["_STATERESPONSE"]._serialized_end = 506
    _globals["_STATEFULPROCESSORCALL"]._serialized_start = 509
    _globals["_STATEFULPROCESSORCALL"]._serialized_end = 994
    _globals["_STATEVARIABLEREQUEST"]._serialized_start = 997
    _globals["_STATEVARIABLEREQUEST"]._serialized_end = 1293
    _globals["_IMPLICITGROUPINGKEYREQUEST"]._serialized_start = 1296
    _globals["_IMPLICITGROUPINGKEYREQUEST"]._serialized_end = 1520
    _globals["_STATECALLCOMMAND"]._serialized_start = 1523
    _globals["_STATECALLCOMMAND"]._serialized_end = 1677
    _globals["_VALUESTATECALL"]._serialized_start = 1680
    _globals["_VALUESTATECALL"]._serialized_end = 2033
    _globals["_LISTSTATECALL"]._serialized_start = 2036
    _globals["_LISTSTATECALL"]._serialized_end = 2564
    _globals["_MAPSTATECALL"]._serialized_start = 2567
    _globals["_MAPSTATECALL"]._serialized_end = 3304
    _globals["_SETIMPLICITKEY"]._serialized_start = 3306
    _globals["_SETIMPLICITKEY"]._serialized_end = 3335
    _globals["_REMOVEIMPLICITKEY"]._serialized_start = 3337
    _globals["_REMOVEIMPLICITKEY"]._serialized_end = 3356
    _globals["_EXISTS"]._serialized_start = 3358
    _globals["_EXISTS"]._serialized_end = 3366
    _globals["_GET"]._serialized_start = 3368
    _globals["_GET"]._serialized_end = 3373
    _globals["_VALUESTATEUPDATE"]._serialized_start = 3375
    _globals["_VALUESTATEUPDATE"]._serialized_end = 3408
    _globals["_CLEAR"]._serialized_start = 3410
    _globals["_CLEAR"]._serialized_end = 3417
    _globals["_LISTSTATEGET"]._serialized_start = 3419
    _globals["_LISTSTATEGET"]._serialized_end = 3453
    _globals["_LISTSTATEPUT"]._serialized_start = 3455
    _globals["_LISTSTATEPUT"]._serialized_end = 3469
    _globals["_APPENDVALUE"]._serialized_start = 3471
    _globals["_APPENDVALUE"]._serialized_end = 3499
    _globals["_APPENDLIST"]._serialized_start = 3501
    _globals["_APPENDLIST"]._serialized_end = 3513
    _globals["_GETVALUE"]._serialized_start = 3515
    _globals["_GETVALUE"]._serialized_end = 3542
    _globals["_CONTAINSKEY"]._serialized_start = 3544
    _globals["_CONTAINSKEY"]._serialized_end = 3574
    _globals["_UPDATEVALUE"]._serialized_start = 3576
    _globals["_UPDATEVALUE"]._serialized_end = 3621
    _globals["_ITERATOR"]._serialized_start = 3623
    _globals["_ITERATOR"]._serialized_end = 3653
    _globals["_KEYS"]._serialized_start = 3655
    _globals["_KEYS"]._serialized_end = 3681
    _globals["_VALUES"]._serialized_start = 3683
    _globals["_VALUES"]._serialized_end = 3711
    _globals["_REMOVEKEY"]._serialized_start = 3713
    _globals["_REMOVEKEY"]._serialized_end = 3741
    _globals["_SETHANDLESTATE"]._serialized_start = 3743
    _globals["_SETHANDLESTATE"]._serialized_end = 3835
    _globals["_TTLCONFIG"]._serialized_start = 3837
    _globals["_TTLCONFIG"]._serialized_end = 3868
# @@protoc_insertion_point(module_scope)
