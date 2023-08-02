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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import uuid
from collections.abc import Generator
from typing import Optional, Dict, Any, Iterator, Iterable, Tuple
from multiprocessing.pool import ThreadPool
import os

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib


class ExecutePlanResponseReattachableIterator(Generator):
    """
    Retryable iterator of ExecutePlanResponses to an ExecutePlan call.

    It can handle situations when:
      - the ExecutePlanResponse stream was broken by retryable network error (governed by
        retryPolicy)
      - the ExecutePlanResponse was gracefully ended by the server without a ResultComplete
        message; this tells the client that there is more, and it should reattach to continue.

    Initial iterator is the result of an ExecutePlan on the request, but it can be reattached with
    ReattachExecute request. ReattachExecute request is provided the responseId of last returned
    ExecutePlanResponse on the iterator to return a new iterator from server that continues after
    that.

    In reattachable execute the server does buffer some responses in case the client needs to
    backtrack. To let server release this buffer sooner, this iterator asynchronously sends
    ReleaseExecute RPCs that instruct the server to release responses that it already processed.

    Note: If the initial ExecutePlan did not even reach the server and execution didn't start,
    the ReattachExecute can still fail with INVALID_HANDLE.OPERATION_NOT_FOUND, failing the whole
    operation.
    """

    _release_thread_pool = ThreadPool(os.cpu_count() if os.cpu_count() else 8)

    def __init__(
        self,
        request: pb2.ExecutePlanRequest,
        stub: grpc_lib.SparkConnectServiceStub,
        retry_policy: Dict[str, Any],
        metadata: Iterable[Tuple[str, str]],
    ):
        self._request = request
        self._retry_policy = retry_policy
        if request.operation_id:
            self._operation_id = request.operation_id
        else:
            # Add operation id, if not present.
            # with operationId set by the client, the client can use it to try to reattach on error
            # even before getting the first response. If the operation in fact didn't even reach the
            # server, that will end with INVALID_HANDLE.OPERATION_NOT_FOUND error.
            self._operation_id = str(uuid.uuid4())

        self._stub = stub
        request.request_options.append(
            pb2.ExecutePlanRequest.RequestOption(
                reattach_options=pb2.ReattachOptions(reattachable=True)
            )
        )
        request.operation_id = self._operation_id
        self._initial_request = request

        # ResponseId of the last response returned by next()
        self._last_returned_response_id: Optional[str] = None

        # True after ResponseComplete message was seen in the stream.
        # Server will always send this message at the end of the stream, if the underlying iterator
        # finishes without producing one, another iterator needs to be reattached.
        self._result_complete = False

        # Initial iterator comes from ExecutePlan request.
        # Note: This is not retried, because no error would ever be thrown here, and GRPC will only
        # throw error on first self._has_next().
        self._iterator: Iterator[pb2.ExecutePlanResponse] = iter(
            self._stub.ExecutePlan(self._initial_request, metadata=metadata)
        )

        # Current item from this iterator.
        self._current: Optional[pb2.ExecutePlanResponse] = None

    def send(self, value: Any) -> pb2.ExecutePlanResponse:
        # will trigger reattach in case the stream completed without result_complete
        if not self._has_next():
            raise StopIteration()

        ret = self._current
        assert ret is not None

        self._last_returned_response_id = ret.response_id
        if ret.HasField("result_complete"):
            self._result_complete = True
            self._release_execute(None)  # release all
        else:
            self._release_execute(self._last_returned_response_id)
        self._current = None
        return ret

    def _has_next(self) -> bool:
        from pyspark.sql.connect.client.core import SparkConnectClient
        from pyspark.sql.connect.client.core import Retrying

        if self._result_complete:
            # After response complete response
            return False
        else:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    # on first try, we use the existing iterator.
                    if not attempt.is_first_try():
                        # on retry, the iterator is borked, so we need a new one
                        self._iterator = iter(
                            self._stub.ReattachExecute(self._create_reattach_execute_request())
                        )

                    if self._current is None:
                        try:
                            self._current = next(self._iterator)
                        except StopIteration:
                            pass

                    has_next = self._current is not None

                    # Graceful reattach:
                    # If iterator ended, but there was no ResponseComplete, it means that
                    # there is more, and we need to reattach. While ResponseComplete didn't
                    # arrive, we keep reattaching.
                    first_loop = True
                    if not self._result_complete and not has_next:
                        while not has_next or first_loop:
                            self._iterator = iter(
                                self._stub.ReattachExecute(self._create_reattach_execute_request())
                            )
                            # shouldn't change
                            assert not self._result_complete
                            try:
                                self._current = next(self._iterator)
                            except StopIteration:
                                pass
                            has_next = self._current is not None
                            if first_loop:
                                # It's possible that the new iterator will be empty, so we need
                                # to loop to get another. Eventually, there will be a non empty
                                # iterator, because there's always a ResultComplete at the end
                                # of the stream.
                                first_loop = False
                    return has_next
            return False

    def _release_execute(self, until_response_id: Optional[str]) -> None:
        """
        Inform the server to release the execution.

        This will send an asynchronous RPC which will not block this iterator, the iterator can
        continue to be consumed.

        Release with untilResponseId informs the server that the iterator has been consumed until
        and including response with that responseId, and these responses can be freed.

        Release with None means that the responses have been completely consumed and informs the
        server that the completed execution can be completely freed.
        """
        from pyspark.sql.connect.client.core import SparkConnectClient
        from pyspark.sql.connect.client.core import Retrying

        request = self._create_release_execute_request(until_response_id)

        def target() -> None:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    self._stub.ReleaseExecute(request)

        ExecutePlanResponseReattachableIterator._release_thread_pool.apply_async(target)

    def _create_reattach_execute_request(self) -> pb2.ReattachExecuteRequest:
        release = pb2.ReattachExecuteRequest(
            session_id=self._initial_request.session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            release.client_type = self._initial_request.client_type

        return release

    def _create_release_execute_request(
        self, until_response_id: Optional[str]
    ) -> pb2.ReleaseExecuteRequest:
        release = pb2.ReleaseExecuteRequest(
            session_id=self._initial_request.session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            release.client_type = self._initial_request.client_type

        if not until_response_id:
            release.release_all.CopyFrom(pb2.ReleaseExecuteRequest.ReleaseAll())
        else:
            release.release_until.response_id = until_response_id

        return release

    def throw(self, type: Any = None, value: Any = None, traceback: Any = None) -> Any:
        super().throw(type, value, traceback)

    def close(self) -> None:
        return super().close()

    def __del__(self) -> None:
        return self.close()
