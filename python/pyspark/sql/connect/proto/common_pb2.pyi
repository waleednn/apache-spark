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
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import typing

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing.final
class StorageLevel(google.protobuf.message.Message):
    """StorageLevel for persisting Datasets/Tables."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    USE_DISK_FIELD_NUMBER: builtins.int
    USE_MEMORY_FIELD_NUMBER: builtins.int
    USE_OFF_HEAP_FIELD_NUMBER: builtins.int
    DESERIALIZED_FIELD_NUMBER: builtins.int
    REPLICATION_FIELD_NUMBER: builtins.int
    use_disk: builtins.bool
    """(Required) Whether the cache should use disk or not."""
    use_memory: builtins.bool
    """(Required) Whether the cache should use memory or not."""
    use_off_heap: builtins.bool
    """(Required) Whether the cache should use off-heap or not."""
    deserialized: builtins.bool
    """(Required) Whether the cached data is deserialized or not."""
    replication: builtins.int
    """(Required) The number of replicas."""
    def __init__(
        self,
        *,
        use_disk: builtins.bool = ...,
        use_memory: builtins.bool = ...,
        use_off_heap: builtins.bool = ...,
        deserialized: builtins.bool = ...,
        replication: builtins.int = ...,
    ) -> None: ...
    def ClearField(
        self,
        field_name: typing.Literal[
            "deserialized",
            b"deserialized",
            "replication",
            b"replication",
            "use_disk",
            b"use_disk",
            "use_memory",
            b"use_memory",
            "use_off_heap",
            b"use_off_heap",
        ],
    ) -> None: ...

global___StorageLevel = StorageLevel

@typing.final
class ResourceInformation(google.protobuf.message.Message):
    """ResourceInformation to hold information about a type of Resource.
    The corresponding class is 'org.apache.spark.resource.ResourceInformation'
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    ADDRESSES_FIELD_NUMBER: builtins.int
    name: builtins.str
    """(Required) The name of the resource"""
    @property
    def addresses(
        self,
    ) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]:
        """(Required) An array of strings describing the addresses of the resource."""
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        addresses: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing.Literal["addresses", b"addresses", "name", b"name"]
    ) -> None: ...

global___ResourceInformation = ResourceInformation

@typing.final
class ExecutorResourceRequest(google.protobuf.message.Message):
    """An executor resource request."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    RESOURCE_NAME_FIELD_NUMBER: builtins.int
    AMOUNT_FIELD_NUMBER: builtins.int
    DISCOVERY_SCRIPT_FIELD_NUMBER: builtins.int
    VENDOR_FIELD_NUMBER: builtins.int
    resource_name: builtins.str
    """(Required) resource name."""
    amount: builtins.int
    """(Required) resource amount requesting."""
    discovery_script: builtins.str
    """Optional script used to discover the resources."""
    vendor: builtins.str
    """Optional vendor, required for some cluster managers."""
    def __init__(
        self,
        *,
        resource_name: builtins.str = ...,
        amount: builtins.int = ...,
        discovery_script: builtins.str | None = ...,
        vendor: builtins.str | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing.Literal[
            "_discovery_script",
            b"_discovery_script",
            "_vendor",
            b"_vendor",
            "discovery_script",
            b"discovery_script",
            "vendor",
            b"vendor",
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing.Literal[
            "_discovery_script",
            b"_discovery_script",
            "_vendor",
            b"_vendor",
            "amount",
            b"amount",
            "discovery_script",
            b"discovery_script",
            "resource_name",
            b"resource_name",
            "vendor",
            b"vendor",
        ],
    ) -> None: ...
    @typing.overload
    def WhichOneof(
        self, oneof_group: typing.Literal["_discovery_script", b"_discovery_script"]
    ) -> typing.Literal["discovery_script"] | None: ...
    @typing.overload
    def WhichOneof(
        self, oneof_group: typing.Literal["_vendor", b"_vendor"]
    ) -> typing.Literal["vendor"] | None: ...

global___ExecutorResourceRequest = ExecutorResourceRequest

@typing.final
class TaskResourceRequest(google.protobuf.message.Message):
    """A task resource request."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    RESOURCE_NAME_FIELD_NUMBER: builtins.int
    AMOUNT_FIELD_NUMBER: builtins.int
    resource_name: builtins.str
    """(Required) resource name."""
    amount: builtins.float
    """(Required) resource amount requesting as a double to support fractional
    resource requests.
    """
    def __init__(
        self,
        *,
        resource_name: builtins.str = ...,
        amount: builtins.float = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing.Literal["amount", b"amount", "resource_name", b"resource_name"]
    ) -> None: ...

global___TaskResourceRequest = TaskResourceRequest

@typing.final
class ResourceProfile(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing.final
    class ExecutorResourcesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___ExecutorResourceRequest: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___ExecutorResourceRequest | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(
            self, field_name: typing.Literal["key", b"key", "value", b"value"]
        ) -> None: ...

    @typing.final
    class TaskResourcesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___TaskResourceRequest: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___TaskResourceRequest | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(
            self, field_name: typing.Literal["key", b"key", "value", b"value"]
        ) -> None: ...

    EXECUTOR_RESOURCES_FIELD_NUMBER: builtins.int
    TASK_RESOURCES_FIELD_NUMBER: builtins.int
    @property
    def executor_resources(
        self,
    ) -> google.protobuf.internal.containers.MessageMap[
        builtins.str, global___ExecutorResourceRequest
    ]:
        """(Optional) Resource requests for executors. Mapped from the resource name
        (e.g., cores, memory, CPU) to its specific request.
        """
    @property
    def task_resources(
        self,
    ) -> google.protobuf.internal.containers.MessageMap[builtins.str, global___TaskResourceRequest]:
        """(Optional) Resource requests for tasks. Mapped from the resource name
        (e.g., cores, memory, CPU) to its specific request.
        """
    def __init__(
        self,
        *,
        executor_resources: collections.abc.Mapping[builtins.str, global___ExecutorResourceRequest]
        | None = ...,
        task_resources: collections.abc.Mapping[builtins.str, global___TaskResourceRequest]
        | None = ...,
    ) -> None: ...
    def ClearField(
        self,
        field_name: typing.Literal[
            "executor_resources", b"executor_resources", "task_resources", b"task_resources"
        ],
    ) -> None: ...

global___ResourceProfile = ResourceProfile

@typing.final
class Origin(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PYTHON_ORIGIN_FIELD_NUMBER: builtins.int
    @property
    def python_origin(self) -> global___PythonOrigin: ...
    def __init__(
        self,
        *,
        python_origin: global___PythonOrigin | None = ...,
    ) -> None: ...
    def HasField(
        self, field_name: typing.Literal["function", b"function", "python_origin", b"python_origin"]
    ) -> builtins.bool: ...
    def ClearField(
        self, field_name: typing.Literal["function", b"function", "python_origin", b"python_origin"]
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing.Literal["function", b"function"]
    ) -> typing.Literal["python_origin"] | None: ...

global___Origin = Origin

@typing.final
class PythonOrigin(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FRAGMENT_FIELD_NUMBER: builtins.int
    CALL_SITE_FIELD_NUMBER: builtins.int
    fragment: builtins.str
    """(Required) Name of the origin, for example, the name of the function"""
    call_site: builtins.str
    """(Required) Callsite to show to end users, for example, stacktrace."""
    def __init__(
        self,
        *,
        fragment: builtins.str = ...,
        call_site: builtins.str = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing.Literal["call_site", b"call_site", "fragment", b"fragment"]
    ) -> None: ...

global___PythonOrigin = PythonOrigin
