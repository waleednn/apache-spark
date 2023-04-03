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

import sys
from typing import cast, overload, Callable, Dict, List, Optional, TYPE_CHECKING, Union

from pyspark.sql.connect.plan import DataSource, LogicalPlan, WriteStreamOperation
import pyspark.sql.connect.proto as pb2
from pyspark.sql.connect.readwriter import OptionUtils, to_str
from pyspark.sql.connect.streaming.query import StreamingQuery
from pyspark.sql.streaming.readwriter import (
    DataStreamReader as PySparkDataStreamReader,
    DataStreamWriter as PySparkDataStreamWriter,
)
from pyspark.sql.types import Row, StructType

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect._typing import OptionalPrimitiveType
    from pyspark.sql.connect.dataframe import DataFrame

__all__ = ["DataStreamReader", "DataStreamWriter"]


class DataStreamReader(OptionUtils):

    def __init__(self, client: "SparkSession") -> None:
        self._format: Optional[str] = None
        self._schema = ""
        self._client = client
        self._options: Dict[str, str] = {}

    def _df(self, plan: LogicalPlan) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        return DataFrame.withPlan(plan, self._client)

    def format(self, source: str) -> "DataStreamReader":
        self._format = source
        return self

    format.__doc__ = PySparkDataStreamReader.format.__doc__

    def schema(self, schema: Union[StructType, str]) -> "DataStreamReader":
        if isinstance(schema, StructType):
            self._schema = schema.json()
        elif isinstance(schema, str):
            self._schema = schema
        else:
            raise TypeError("schema should be StructType or string")
        return self

    schema.__doc__ = PySparkDataStreamReader.schema.__doc__

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataStreamReader":
        self._options[key] = str(value)
        return self

    option.__doc__ = PySparkDataStreamReader.option.__doc__

    def options(self, **options: "OptionalPrimitiveType") -> "DataStreamReader":
        for k in options:
            self.option(k, to_str(options[k]))
        return self

    options.__doc__ = PySparkDataStreamReader.options.__doc__

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            if type(path) != str or len(path.strip()) == 0:
                raise ValueError(
                    "If the path is provided for stream, it needs to be a "
                    + "non-empty string. List of paths are not supported."
                )

        plan = DataSource(
            format=self._format,
            schema=self._schema,
            options=self._options,
            paths=[path] if path else None,
            is_streaming=True
        )

        return self._df(plan)

    load.__doc__ = PySparkDataStreamReader.load.__doc__

    def json(
        self,
        path: str,
        schema: Optional[Union[StructType, str]] = None,
        primitivesAsString: Optional[Union[bool, str]] = None,
        prefersDecimal: Optional[Union[bool, str]] = None,
        allowComments: Optional[Union[bool, str]] = None,
        allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allowSingleQuotes: Optional[Union[bool, str]] = None,
        allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        lineSep: Optional[str] = None,
        locale: Optional[str] = None,
        dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        encoding: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        self._set_opts(
            schema=schema,
            primitivesAsString=primitivesAsString,
            prefersDecimal=prefersDecimal,
            allowComments=allowComments,
            allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes,
            allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars,
            lineSep=lineSep,
            locale=locale,
            dropFieldIfAllNull=dropFieldIfAllNull,
            encoding=encoding,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            allowNonNumericNumbers=allowNonNumericNumbers,
        )
        if isinstance(path, str):
            return self.load(path=path, format="json")
        else:
            raise TypeError("path can be only a single string")

    json.__doc__ = PySparkDataStreamReader.json.__doc__

    # def orc() TODO
    # def parquet() TODO
    # def text() TODO

    def csv(
        self,
        path: str,
        schema: Optional[Union[StructType, str]] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        comment: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        nanValue: Optional[str] = None,
        positiveInf: Optional[str] = None,
        negativeInf: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        maxColumns: Optional[Union[int, str]] = None,
        maxCharsPerColumn: Optional[Union[int, str]] = None,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[Union[bool, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        self._set_opts(
            schema=schema,
            sep=sep,
            encoding=encoding,
            quote=quote,
            escape=escape,
            comment=comment,
            header=header,
            inferSchema=inferSchema,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            nullValue=nullValue,
            nanValue=nanValue,
            positiveInf=positiveInf,
            negativeInf=negativeInf,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )
        if isinstance(path, str):
            return self.load(path=path, format="csv")
        else:
            raise TypeError("path can be only a single string")

    csv.__doc__ = PySparkDataStreamReader.csv.__doc__

    # def table() TODO. Use Read(table_name) relation.


DataStreamReader.__doc__ = PySparkDataStreamReader.__doc__


class DataStreamWriter:

    def __init__(self, plan: "LogicalPlan", session: "SparkSession") -> None:
        self._session = session
        self._write_stream = WriteStreamOperation(plan)
        self._write_proto = self._write_stream.write_op

    def outputMode(self, outputMode: str) -> "DataStreamWriter":
        self._write_proto.output_mode = outputMode
        return self

    outputMode.__doc__ = PySparkDataStreamWriter.outputMode.__doc__

    def format(self, source: str) -> "DataStreamWriter":
        self._write_proto.format = source
        return self

    format.__doc__ = PySparkDataStreamWriter.format.__doc__

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataStreamWriter":
        self._write_proto.options[key] = to_str(value)
        return self

    option.__doc__ = PySparkDataStreamWriter.option.__doc__
    
    def options(self, **options: "OptionalPrimitiveType") -> "DataStreamWriter":
        for k in options:
            self.option(k, options[k])        
        return self

    options.__doc__ = PySparkDataStreamWriter.options.__doc__
    
    @overload
    def partitionBy(self, *cols: str) -> "DataStreamWriter":
        ...

    @overload
    def partitionBy(self, __cols: List[str]) -> "DataStreamWriter":
        ...

    def partitionBy(self, *cols: str) -> "DataStreamWriter":  # type: ignore[misc]
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._write_proto.partitioning_cols = cast(List[str], cols)
        return self

    partitionBy.__doc__ = PySparkDataStreamWriter.partitionBy.__doc__

    def queryName(self, queryName: str) -> "DataStreamWriter":
        self._write_proto.query_name = queryName
        return self

    queryName.__doc__ = PySparkDataStreamWriter.queryName.__doc__

    @overload
    def trigger(self, *, processingTime: str) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, once: bool) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, continuous: str) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, availableNow: bool) -> "DataStreamWriter":
        ...

    def trigger(
        self,
        *,
        processingTime: Optional[str] = None,
        once: Optional[bool] = None,
        continuous: Optional[str] = None,
        availableNow: Optional[bool] = None,
    ) -> "DataStreamWriter":
        params = [processingTime, once, continuous, availableNow]

        if params.count(None) == 4:
            raise ValueError("No trigger provided")
        elif params.count(None) < 3:
            raise ValueError("Multiple triggers not allowed.")

        if processingTime is not None:
            if type(processingTime) != str or len(processingTime.strip()) == 0:
                raise ValueError(
                    "Value for processingTime must be a non empty string. Got: %s" % processingTime
                )
            self._write_proto.processing_time_interval = processingTime.strip()

        elif once is not None:
            if once is not True:
                raise ValueError("Value for once must be True. Got: %s" % once)
            self._write_proto.one_time = True

        elif continuous is not None:
            if type(continuous) != str or len(continuous.strip()) == 0:
                raise ValueError(
                    "Value for continuous must be a non empty string. Got: %s" % continuous
                )
            self._write_proto.continuous_checkpoint_interval = continuous.strip()

        else:
            if availableNow is not True:
                raise ValueError("Value for availableNow must be True. Got: %s" % availableNow)
            self._write_proto.available_now = True

        return self

    trigger.__doc__ = PySparkDataStreamWriter.trigger.__doc__

    @overload
    def foreach(self, f: Callable[[Row], None]) -> "DataStreamWriter":
        ...

    @overload
    def foreach(self, f: "SupportsProcess") -> "DataStreamWriter":
        ...

    def foreach(self, f: Union[Callable[[Row], None], "SupportsProcess"]) -> "DataStreamWriter":
        raise NotImplementedError("foreach() is not implemented.")

    foreach.__doc__ = PySparkDataStreamWriter.foreach.__doc__

    def _start_internal(
        self,
        path: Optional[str] = None,
        tableName: Optional[str] = None,
        format: Optional[str] = None,
        outputMode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        queryName: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> StreamingQuery:
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)
        if path:
            self._write_proto.path = path
        if tableName:
            self._write_proto.table_name = tableName

        cmd = self._write_stream.command(self._session.client)
        (_, properties) = self._session.client.execute_command(cmd)

        start_result = cast(pb2.WriteStreamOperationStartResult, properties["write_stream_operation_start_result"])
        return StreamingQuery(
            session=self._session,
            queryId=start_result.query_id,
            runId=start_result.run_id,
            name=start_result.name
        )

    def start(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        outputMode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        queryName: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> StreamingQuery:
        return self._start_internal(
            path=path,
            format=format,
            outputMode=outputMode,
            partitionBy=partitionBy,
            queryName=queryName,
            **options,
        )

    start.__doc__ = PySparkDataStreamWriter.start.__doc__

    def toTable(
        self,
        tableName: str,
        format: Optional[str] = None,
        outputMode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        queryName: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> StreamingQuery:
        return self._start_internal(
            tableName=tableName,
            format=format,
            outputMode=outputMode,
            partitionBy=partitionBy,
            queryName=queryName,
            **options,
        )

    toTable.__doc__ = PySparkDataStreamWriter.toTable.__doc__


def _test() -> None:
    import doctest
    import os
    from pyspark.sql import SparkSession
    import pyspark.sql.streaming.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.readwriter.__dict__.copy()
    globs["spark"] = (
        SparkSession.builder
        .appName("sql.connect.streaming.readwriter tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.streaming.readwriter,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
