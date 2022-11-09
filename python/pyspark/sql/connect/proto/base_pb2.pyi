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
"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import builtins
import collections.abc
import google.protobuf.any_pb2
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import pyspark.sql.connect.proto.commands_pb2
import pyspark.sql.connect.proto.relations_pb2
import pyspark.sql.connect.proto.types_pb2
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class Plan(google.protobuf.message.Message):
    """A [[Plan]] is the structure that carries the runtime information for the execution from the
    client to the server. A [[Plan]] can either be of the type [[Relation]] which is a reference
    to the underlying logical plan or it can be of the [[Command]] type that is used to execute
    commands on the server.
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ROOT_FIELD_NUMBER: builtins.int
    COMMAND_FIELD_NUMBER: builtins.int
    @property
    def root(self) -> pyspark.sql.connect.proto.relations_pb2.Relation: ...
    @property
    def command(self) -> pyspark.sql.connect.proto.commands_pb2.Command: ...
    def __init__(
        self,
        *,
        root: pyspark.sql.connect.proto.relations_pb2.Relation | None = ...,
        command: pyspark.sql.connect.proto.commands_pb2.Command | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "command", b"command", "op_type", b"op_type", "root", b"root"
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "command", b"command", "op_type", b"op_type", "root", b"root"
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["op_type", b"op_type"]
    ) -> typing_extensions.Literal["root", "command"] | None: ...

global___Plan = Plan

class Request(google.protobuf.message.Message):
    """A request to be executed by the service."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class UserContext(google.protobuf.message.Message):
        """User Context is used to refer to one particular user session that is executing
        queries in the backend.
        """

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        USER_ID_FIELD_NUMBER: builtins.int
        USER_NAME_FIELD_NUMBER: builtins.int
        EXTENSIONS_FIELD_NUMBER: builtins.int
        user_id: builtins.str
        user_name: builtins.str
        @property
        def extensions(
            self,
        ) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[
            google.protobuf.any_pb2.Any
        ]:
            """To extend the existing user context message that is used to identify incoming requests,
            Spark Connect leverages the Any protobuf type that can be used to inject arbitrary other
            messages into this message. Extensions are stored as a `repeated` type to be able to
            handle multiple active extensions.
            """
        def __init__(
            self,
            *,
            user_id: builtins.str = ...,
            user_name: builtins.str = ...,
            extensions: collections.abc.Iterable[google.protobuf.any_pb2.Any] | None = ...,
        ) -> None: ...
        def ClearField(
            self,
            field_name: typing_extensions.Literal[
                "extensions", b"extensions", "user_id", b"user_id", "user_name", b"user_name"
            ],
        ) -> None: ...

    CLIENT_ID_FIELD_NUMBER: builtins.int
    USER_CONTEXT_FIELD_NUMBER: builtins.int
    PLAN_FIELD_NUMBER: builtins.int
    client_id: builtins.str
    """The client_id is set by the client to be able to collate streaming responses from
    different queries.
    """
    @property
    def user_context(self) -> global___Request.UserContext:
        """User context"""
    @property
    def plan(self) -> global___Plan:
        """The logical plan to be executed / analyzed."""
    def __init__(
        self,
        *,
        client_id: builtins.str = ...,
        user_context: global___Request.UserContext | None = ...,
        plan: global___Plan | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal["plan", b"plan", "user_context", b"user_context"],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "client_id", b"client_id", "plan", b"plan", "user_context", b"user_context"
        ],
    ) -> None: ...

global___Request = Request

class Response(google.protobuf.message.Message):
    """The response of a query, can be one or more for each request. Responses belonging to the
    same input query, carry the same `client_id`.
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class ArrowBatch(google.protobuf.message.Message):
        """Batch results of metrics."""

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        ROW_COUNT_FIELD_NUMBER: builtins.int
        UNCOMPRESSED_BYTES_FIELD_NUMBER: builtins.int
        COMPRESSED_BYTES_FIELD_NUMBER: builtins.int
        DATA_FIELD_NUMBER: builtins.int
        row_count: builtins.int
        uncompressed_bytes: builtins.int
        compressed_bytes: builtins.int
        data: builtins.bytes
        def __init__(
            self,
            *,
            row_count: builtins.int = ...,
            uncompressed_bytes: builtins.int = ...,
            compressed_bytes: builtins.int = ...,
            data: builtins.bytes = ...,
        ) -> None: ...
        def ClearField(
            self,
            field_name: typing_extensions.Literal[
                "compressed_bytes",
                b"compressed_bytes",
                "data",
                b"data",
                "row_count",
                b"row_count",
                "uncompressed_bytes",
                b"uncompressed_bytes",
            ],
        ) -> None: ...

    class JSONBatch(google.protobuf.message.Message):
        """Message type when the result is returned as JSON. This is essentially a bulk wrapper
        for the JSON result of a Spark DataFrame. All rows are returned in the JSON record format
        of `{col -> row}`.
        """

        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        ROW_COUNT_FIELD_NUMBER: builtins.int
        DATA_FIELD_NUMBER: builtins.int
        row_count: builtins.int
        data: builtins.bytes
        def __init__(
            self,
            *,
            row_count: builtins.int = ...,
            data: builtins.bytes = ...,
        ) -> None: ...
        def ClearField(
            self, field_name: typing_extensions.Literal["data", b"data", "row_count", b"row_count"]
        ) -> None: ...

    class Metrics(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        class MetricObject(google.protobuf.message.Message):
            DESCRIPTOR: google.protobuf.descriptor.Descriptor

            class ExecutionMetricsEntry(google.protobuf.message.Message):
                DESCRIPTOR: google.protobuf.descriptor.Descriptor

                KEY_FIELD_NUMBER: builtins.int
                VALUE_FIELD_NUMBER: builtins.int
                key: builtins.str
                @property
                def value(self) -> global___Response.Metrics.MetricValue: ...
                def __init__(
                    self,
                    *,
                    key: builtins.str = ...,
                    value: global___Response.Metrics.MetricValue | None = ...,
                ) -> None: ...
                def HasField(
                    self, field_name: typing_extensions.Literal["value", b"value"]
                ) -> builtins.bool: ...
                def ClearField(
                    self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]
                ) -> None: ...

            NAME_FIELD_NUMBER: builtins.int
            PLAN_ID_FIELD_NUMBER: builtins.int
            PARENT_FIELD_NUMBER: builtins.int
            EXECUTION_METRICS_FIELD_NUMBER: builtins.int
            name: builtins.str
            plan_id: builtins.int
            parent: builtins.int
            @property
            def execution_metrics(
                self,
            ) -> google.protobuf.internal.containers.MessageMap[
                builtins.str, global___Response.Metrics.MetricValue
            ]: ...
            def __init__(
                self,
                *,
                name: builtins.str = ...,
                plan_id: builtins.int = ...,
                parent: builtins.int = ...,
                execution_metrics: collections.abc.Mapping[
                    builtins.str, global___Response.Metrics.MetricValue
                ]
                | None = ...,
            ) -> None: ...
            def ClearField(
                self,
                field_name: typing_extensions.Literal[
                    "execution_metrics",
                    b"execution_metrics",
                    "name",
                    b"name",
                    "parent",
                    b"parent",
                    "plan_id",
                    b"plan_id",
                ],
            ) -> None: ...

        class MetricValue(google.protobuf.message.Message):
            DESCRIPTOR: google.protobuf.descriptor.Descriptor

            NAME_FIELD_NUMBER: builtins.int
            VALUE_FIELD_NUMBER: builtins.int
            METRIC_TYPE_FIELD_NUMBER: builtins.int
            name: builtins.str
            value: builtins.int
            metric_type: builtins.str
            def __init__(
                self,
                *,
                name: builtins.str = ...,
                value: builtins.int = ...,
                metric_type: builtins.str = ...,
            ) -> None: ...
            def ClearField(
                self,
                field_name: typing_extensions.Literal[
                    "metric_type", b"metric_type", "name", b"name", "value", b"value"
                ],
            ) -> None: ...

        METRICS_FIELD_NUMBER: builtins.int
        @property
        def metrics(
            self,
        ) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[
            global___Response.Metrics.MetricObject
        ]: ...
        def __init__(
            self,
            *,
            metrics: collections.abc.Iterable[global___Response.Metrics.MetricObject] | None = ...,
        ) -> None: ...
        def ClearField(
            self, field_name: typing_extensions.Literal["metrics", b"metrics"]
        ) -> None: ...

    CLIENT_ID_FIELD_NUMBER: builtins.int
    ARROW_BATCH_FIELD_NUMBER: builtins.int
    JSON_BATCH_FIELD_NUMBER: builtins.int
    METRICS_FIELD_NUMBER: builtins.int
    client_id: builtins.str
    @property
    def arrow_batch(self) -> global___Response.ArrowBatch: ...
    @property
    def json_batch(self) -> global___Response.JSONBatch: ...
    @property
    def metrics(self) -> global___Response.Metrics:
        """Metrics for the query execution. Typically, this field is only present in the last
        batch of results and then represent the overall state of the query execution.
        """
    def __init__(
        self,
        *,
        client_id: builtins.str = ...,
        arrow_batch: global___Response.ArrowBatch | None = ...,
        json_batch: global___Response.JSONBatch | None = ...,
        metrics: global___Response.Metrics | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "arrow_batch",
            b"arrow_batch",
            "json_batch",
            b"json_batch",
            "metrics",
            b"metrics",
            "result_type",
            b"result_type",
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "arrow_batch",
            b"arrow_batch",
            "client_id",
            b"client_id",
            "json_batch",
            b"json_batch",
            "metrics",
            b"metrics",
            "result_type",
            b"result_type",
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["result_type", b"result_type"]
    ) -> typing_extensions.Literal["arrow_batch", "json_batch"] | None: ...

global___Response = Response

class AnalyzeResponse(google.protobuf.message.Message):
    """Response to performing analysis of the query. Contains relevant metadata to be able to
    reason about the performance.
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    CLIENT_ID_FIELD_NUMBER: builtins.int
    SCHEMA_FIELD_NUMBER: builtins.int
    EXPLAIN_STRING_FIELD_NUMBER: builtins.int
    client_id: builtins.str
    @property
    def schema(self) -> pyspark.sql.connect.proto.types_pb2.DataType: ...
    explain_string: builtins.str
    """The extended explain string as produced by Spark."""
    def __init__(
        self,
        *,
        client_id: builtins.str = ...,
        schema: pyspark.sql.connect.proto.types_pb2.DataType | None = ...,
        explain_string: builtins.str = ...,
    ) -> None: ...
    def HasField(
        self, field_name: typing_extensions.Literal["schema", b"schema"]
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "client_id", b"client_id", "explain_string", b"explain_string", "schema", b"schema"
        ],
    ) -> None: ...

global___AnalyzeResponse = AnalyzeResponse
