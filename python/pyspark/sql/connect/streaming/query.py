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

import json
import time
from typing import TYPE_CHECKING, Any, cast, Dict, List, Optional

from pyspark.errors import StreamingQueryException
import pyspark.sql.connect.proto as pb2
from pyspark.sql.streaming.query import (
    StreamingQuery as PySparkStreamingQuery,
)
from pyspark.errors.exceptions.captured import (
    StreamingQueryException as CapturedStreamingQueryException,
)

__all__ = [
    "StreamingQuery",  # TODO(SPARK-43032): "StreamingQueryManager"
]

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


class StreamingQuery:
    def __init__(
        self, session: "SparkSession", queryId: str, runId: str, name: Optional[str] = None
    ) -> None:
        self._session = session
        self._query_id = queryId
        self._run_id = runId
        self._name = name

    @property
    def id(self) -> str:
        return self._query_id

    id.__doc__ = PySparkStreamingQuery.id.__doc__

    @property
    def runId(self) -> str:
        return self._run_id

    runId.__doc__ = PySparkStreamingQuery.runId.__doc__

    @property
    def name(self) -> Optional[str]:
        return self._name

    name.__doc__ = PySparkStreamingQuery.name.__doc__

    @property
    def isActive(self) -> bool:
        return self._fetch_status().is_active

    isActive.__doc__ = PySparkStreamingQuery.isActive.__doc__

    def _execute_await_termination_cmd(self, timeout: int = 10) -> bool:
        cmd = pb2.StreamingQueryCommand()
        cmd.await_termination.timeout_ms = timeout
        terminated = self._execute_streaming_query_cmd(cmd).await_termination.terminated
        return terminated
    
    def _await_termination(self, timeoutMs: Optional[int]) -> Optional[bool]:
        terminated = False
        if timeoutMs is None:
            while not terminated:
                terminated = self._execute_await_termination_cmd()
        else:
            reqTimeoutMs = min(timeoutMs, 10)
            while timeoutMs > 0 and not terminated:
                start = time.time()
                terminated = self._execute_await_termination_cmd(reqTimeoutMs)
                end = time.time()
                timeoutMs = (end - start) * 1000
            return terminated

    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
        return self._await_termination(int(timeout * 1000))

    awaitTermination.__doc__ = PySparkStreamingQuery.awaitTermination.__doc__

    @property
    def status(self) -> Dict[str, Any]:
        proto = self._fetch_status()
        return {
            "message": proto.status_message,
            "isDataAvailable": proto.is_data_available,
            "isTriggerActive": proto.is_trigger_active,
        }

    status.__doc__ = PySparkStreamingQuery.status.__doc__

    @property
    def recentProgress(self) -> List[Dict[str, Any]]:
        cmd = pb2.StreamingQueryCommand()
        cmd.recent_progress = True
        progress = self._execute_streaming_query_cmd(cmd).recent_progress.recent_progress_json
        return [json.loads(p) for p in progress]

    recentProgress.__doc__ = PySparkStreamingQuery.recentProgress.__doc__

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        cmd = pb2.StreamingQueryCommand()
        cmd.last_progress = True
        progress = self._execute_streaming_query_cmd(cmd).recent_progress.recent_progress_json
        if len(progress) > 0:
            return json.loads(progress[-1])
        else:
            return None

    lastProgress.__doc__ = PySparkStreamingQuery.lastProgress.__doc__

    def processAllAvailable(self) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.process_all_available = True
        self._execute_streaming_query_cmd(cmd)

    processAllAvailable.__doc__ = PySparkStreamingQuery.processAllAvailable.__doc__

    def stop(self) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.stop = True
        self._execute_streaming_query_cmd(cmd)

    stop.__doc__ = PySparkStreamingQuery.stop.__doc__

    def explain(self, extended: bool = False) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.explain.extended = extended
        result = self._execute_streaming_query_cmd(cmd).explain.result
        print(result)

    explain.__doc__ = PySparkStreamingQuery.explain.__doc__

    def exception(self) -> Optional[StreamingQueryException]:
        cmd = pb2.StreamingQueryCommand()
        cmd.exception = True
        result = self._execute_streaming_query_cmd(cmd).exception.result
        if not result.has_exception:
            return None
        else:
            msg = result.message.split(": ", 1)[1]  # Drop the Java StreamingQueryException type info
            stackTrace = "\n\t at ".join(map(lambda x: x.toString(), result.stack_trace))
            return CapturedStreamingQueryException(msg, stackTrace, result.cause)

    exception.__doc__ = PySparkStreamingQuery.exception.__doc__

    def _fetch_status(self) -> pb2.StreamingQueryCommandResult.StatusResult:
        cmd = pb2.StreamingQueryCommand()
        cmd.status = True
        return self._execute_streaming_query_cmd(cmd).status

    def _execute_streaming_query_cmd(
        self, cmd: pb2.StreamingQueryCommand
    ) -> pb2.StreamingQueryCommandResult:
        cmd.query_id.id = self._query_id
        cmd.query_id.run_id = self._run_id
        exec_cmd = pb2.Command()
        exec_cmd.streaming_query_command.CopyFrom(cmd)
        (_, properties) = self._session.client.execute_command(exec_cmd)
        return cast(pb2.StreamingQueryCommandResult, properties["streaming_query_command_result"])


# TODO(SPARK-43032) class StreamingQueryManager:


def _test() -> None:
    # TODO(SPARK-43031): port _test() from legacy query.py.
    pass


if __name__ == "__main__":
    # TODO(SPARK-43031): Add this file dev/sparktestsupport/modules.py to enable testing in CI.
    _test()
