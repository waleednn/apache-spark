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
import inspect
import os
import sys
from typing import IO, Iterable, Iterator

from pyspark.accumulators import _accumulatorRegistry
from pyspark.sql.connect.conversion import ArrowTableToRowsConversion
from pyspark.errors import PySparkAssertionError, PySparkRuntimeError, PySparkTypeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import (
    read_int,
    write_int,
    SpecialLengths,
)
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, WriterCommitMessage
from pyspark.sql.types import (
    _parse_datatype_json_string,
    StructType,
    StructField,
    BinaryType,
    _create_row,
)
from pyspark.util import handle_worker_exception
from pyspark.worker_util import (
    check_python_version,
    read_command,
    pickleSer,
    send_accumulator_updates,
    setup_broadcasts,
    setup_memory_limits,
    setup_spark_files,
    utf8_deserializer,
)


def main(infile: IO, outfile: IO) -> None:
    """
    Main method for saving into a Python data source.

    This process is invoked from the `SaveIntoPythonDataSourceRunner.runInPython` method
    in the optimizer rule `PythonDataSourceWrites` in JVM. This process is responsible for
    creating a `DataSource` object and a DataSourceWriter instance, and send information
    needed back to the JVM.

    The JVM sends the following information to this process:
    - a `DataSource` class representing the data source to be created.
    - a provider name in string.
    - a schema in json string.
    - a dictionary of options in string.

    This process first creates a `DataSource` instance and then a `DataSourceWriter`
    instance and send a function using the writer instance that can be used
    in mapInPandas/mapInArrow back to the JVM.
    """
    try:
        check_python_version(infile)

        memory_limit_mb = int(os.environ.get("PYSPARK_PLANNER_MEMORY_MB", "-1"))
        setup_memory_limits(memory_limit_mb)

        setup_spark_files(infile)
        setup_broadcasts(infile)

        _accumulatorRegistry.clear()

        # Receive the data source class.
        data_source_cls = read_command(pickleSer, infile)
        if not (isinstance(data_source_cls, type) and issubclass(data_source_cls, DataSource)):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "a subclass of DataSource",
                    "actual": f"'{type(data_source_cls).__name__}'",
                },
            )

        # Check the name method is a class method.
        if not inspect.ismethod(data_source_cls.name):
            raise PySparkTypeError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "'name()' method to be a classmethod",
                    "actual": f"'{type(data_source_cls.name).__name__}'",
                },
            )

        # Receive the provider name.
        provider = utf8_deserializer.loads(infile)

        # Check if the provider name matches the data source's name.
        if provider.lower() != data_source_cls.name().lower():
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": f"provider with name {data_source_cls.name()}",
                    "actual": f"'{provider}'",
                },
            )

        # Receive the output schema
        schema = _parse_datatype_json_string(utf8_deserializer.loads(infile))
        if not isinstance(schema, StructType):
            raise PySparkAssertionError(
                error_class="PYTHON_DATA_SOURCE_TYPE_MISMATCH",
                message_parameters={
                    "expected": "the schema to be a 'StructType'",
                    "actual": f"'{type(data_source_cls).__name__}'",
                },
            )

        # Receive the options.
        options = dict()
        num_options = read_int(infile)
        for _ in range(num_options):
            key = utf8_deserializer.loads(infile)
            value = utf8_deserializer.loads(infile)
            options[key] = value

        # Receive the save mode.
        save_mode = utf8_deserializer.loads(infile)

        # Instantiate a data source.
        # TODO(SPARK-45927) Update the data source class constructor.
        try:
            data_source = data_source_cls(
                paths=[],
                userSpecifiedSchema=schema,
                options=options,
            )
        except Exception as e:
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_CREATE_ERROR",
                message_parameters={"type": "instance", "error": str(e)},
            )

        # Instantiate the data source writer.
        try:
            writer = data_source.writer(schema, save_mode)
        except Exception as e:
            raise PySparkRuntimeError(
                error_class="PYTHON_DATA_SOURCE_CREATE_ERROR",
                message_parameters={"type": "writer", "error": str(e)},
            )

        # Create a function that can be used in mapInArrow.
        import pyarrow as pa

        converters = [
            ArrowTableToRowsConversion._create_converter(f.dataType) for f in schema.fields
        ]
        fields = schema.fieldNames()

        return_type = StructType([StructField("message", BinaryType(), True)])

        def data_source_write_func(iterator: Iterable[pa.RecordBatch]) -> Iterable[pa.RecordBatch]:
            def batch_to_rows() -> Iterator[Row]:
                for batch in iterator:
                    columns = [column.to_pylist() for column in batch.columns]
                    for row in range(0, batch.num_rows):
                        values = [
                            converters[col](columns[col][row]) for col in range(batch.num_columns)
                        ]
                        yield _create_row(fields=fields, values=values)

            res = writer.write(batch_to_rows())

            # Check the commit message has the right type.
            if not isinstance(res, WriterCommitMessage):
                raise PySparkRuntimeError(
                    error_class="PYTHON_DATA_SOURCE_WRITE_ERROR",
                    message_parameters={
                        "error": f"return type of the `write` method must be "
                        f"an instance of WriterCommitMessage, but got {type(res)}"
                    },
                )

            # Serialize the commit message and return it.
            pickled = pickleSer.dumps(res)

            # Return the commit message.
            messages = pa.array([pickled])
            yield pa.record_batch([messages], names=["message"])

        # Return the pickled write UDF.
        command = (data_source_write_func, return_type)
        pickleSer._write_with_length(command, outfile)

    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)

    send_accumulator_updates(outfile)

    # check end of stream
    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    main(sock_file, sock_file)
