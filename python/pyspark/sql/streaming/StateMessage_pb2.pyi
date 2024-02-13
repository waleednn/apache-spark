"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _HandleState:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _HandleStateEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_HandleState.ValueType], builtins.type):  # noqa: F821
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    CREATED: _HandleState.ValueType  # 0
    INITIALIZED: _HandleState.ValueType  # 1
    DATA_PROCESSED: _HandleState.ValueType  # 2
    CLOSED: _HandleState.ValueType  # 3

class HandleState(_HandleState, metaclass=_HandleStateEnumTypeWrapper): ...

CREATED: HandleState.ValueType  # 0
INITIALIZED: HandleState.ValueType  # 1
DATA_PROCESSED: HandleState.ValueType  # 2
CLOSED: HandleState.ValueType  # 3
global___HandleState = HandleState

class StateRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    VERSION_FIELD_NUMBER: builtins.int
    STATEFULPROCESSORHANDLECALL_FIELD_NUMBER: builtins.int
    LISTSTATECALL_FIELD_NUMBER: builtins.int
    version: builtins.int
    @property
    def statefulProcessorHandleCall(self) -> global___StatefulProcessorHandleCall: ...
    @property
    def listStateCall(self) -> global___ListStateCall: ...
    def __init__(
        self,
        *,
        version: builtins.int = ...,
        statefulProcessorHandleCall: global___StatefulProcessorHandleCall | None = ...,
        listStateCall: global___ListStateCall | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["listStateCall", b"listStateCall", "method", b"method", "statefulProcessorHandleCall", b"statefulProcessorHandleCall"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["listStateCall", b"listStateCall", "method", b"method", "statefulProcessorHandleCall", b"statefulProcessorHandleCall", "version", b"version"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["method", b"method"]) -> typing_extensions.Literal["statefulProcessorHandleCall", "listStateCall"] | None: ...

global___StateRequest = StateRequest

class StateResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    STATUSCODE_FIELD_NUMBER: builtins.int
    ERRORMESSAGE_FIELD_NUMBER: builtins.int
    statusCode: builtins.int
    errorMessage: builtins.str
    def __init__(
        self,
        *,
        statusCode: builtins.int = ...,
        errorMessage: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["errorMessage", b"errorMessage", "statusCode", b"statusCode"]) -> None: ...

global___StateResponse = StateResponse

class StatefulProcessorHandleCall(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SETHANDLESTATE_FIELD_NUMBER: builtins.int
    GETLISTSTATE_FIELD_NUMBER: builtins.int
    @property
    def setHandleState(self) -> global___SetHandleState: ...
    @property
    def getListState(self) -> global___GetListState: ...
    def __init__(
        self,
        *,
        setHandleState: global___SetHandleState | None = ...,
        getListState: global___GetListState | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["getListState", b"getListState", "method", b"method", "setHandleState", b"setHandleState"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["getListState", b"getListState", "method", b"method", "setHandleState", b"setHandleState"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["method", b"method"]) -> typing_extensions.Literal["setHandleState", "getListState"] | None: ...

global___StatefulProcessorHandleCall = StatefulProcessorHandleCall

class GetListState(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    STATENAME_FIELD_NUMBER: builtins.int
    SCHEMA_FIELD_NUMBER: builtins.int
    stateName: builtins.str
    schema: builtins.str
    def __init__(
        self,
        *,
        stateName: builtins.str = ...,
        schema: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["schema", b"schema", "stateName", b"stateName"]) -> None: ...

global___GetListState = GetListState

class ListStateCall(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    EXISTS_FIELD_NUMBER: builtins.int
    GET_FIELD_NUMBER: builtins.int
    UPDATE_FIELD_NUMBER: builtins.int
    @property
    def exists(self) -> global___Exists: ...
    @property
    def get(self) -> global___Get: ...
    @property
    def update(self) -> global___Update: ...
    def __init__(
        self,
        *,
        exists: global___Exists | None = ...,
        get: global___Get | None = ...,
        update: global___Update | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["exists", b"exists", "get", b"get", "method", b"method", "update", b"update"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["exists", b"exists", "get", b"get", "method", b"method", "update", b"update"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["method", b"method"]) -> typing_extensions.Literal["exists", "get", "update"] | None: ...

global___ListStateCall = ListStateCall

class Exists(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Exists = Exists

class Get(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Get = Get

class Update(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Update = Update

class SetHandleState(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    STATE_FIELD_NUMBER: builtins.int
    state: global___HandleState.ValueType
    def __init__(
        self,
        *,
        state: global___HandleState.ValueType = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["state", b"state"]) -> None: ...

global___SetHandleState = SetHandleState
