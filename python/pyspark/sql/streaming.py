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

import sys
import json

from py4j.java_gateway import java_import

from pyspark import since, keyword_only
from pyspark.sql.column import _to_seq
from pyspark.sql.readwriter import OptionUtils, to_str
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.utils import ForeachBatchFunction, StreamingQueryException

__all__ = ["StreamingQuery", "StreamingQueryManager", "DataStreamReader", "DataStreamWriter"]


class StreamingQuery(object):
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jsq):
        self._jsq = jsq

    @property
    @since(2.0)
    def id(self):
        """Returns the unique id of this query that persists across restarts from checkpoint data.
        That is, this id is generated when a query is started for the first time, and
        will be the same every time it is restarted from checkpoint data.
        There can only be one query with the same id active in a Spark cluster.
        Also see, `runId`.
        """
        return self._jsq.id().toString()

    @property
    @since(2.1)
    def runId(self):
        """Returns the unique id of this query that does not persist across restarts. That is, every
        query that is started (or restarted from checkpoint) will have a different runId.
        """
        return self._jsq.runId().toString()

    @property
    @since(2.0)
    def name(self):
        """Returns the user-specified name of the query, or null if not specified.
        This name can be specified in the `org.apache.spark.sql.streaming.DataStreamWriter`
        as `dataframe.writeStream.queryName("query").start()`.
        This name, if set, must be unique across all active queries.
        """
        return self._jsq.name()

    @property
    @since(2.0)
    def isActive(self):
        """Whether this streaming query is currently active or not.
        """
        return self._jsq.isActive()

    @since(2.0)
    def awaitTermination(self, timeout=None):
        """Waits for the termination of `this` query, either by :func:`query.stop()` or by an
        exception. If the query has terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If the query has terminated, then all subsequent calls to this method will either return
        immediately (if the query was terminated by :func:`stop()`), or throw the exception
        immediately (if the query has terminated with exception).

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jsq.awaitTermination(int(timeout * 1000))
        else:
            return self._jsq.awaitTermination()

    @property
    @since(2.1)
    def status(self):
        """
        Returns the current status of the query.
        """
        return json.loads(self._jsq.status().json())

    @property
    @since(2.1)
    def recentProgress(self):
        """Returns an array of the most recent [[StreamingQueryProgress]] updates for this query.
        The number of progress updates retained for each stream is configured by Spark session
        configuration `spark.sql.streaming.numRecentProgressUpdates`.
        """
        return [json.loads(p.json()) for p in self._jsq.recentProgress()]

    @property
    def lastProgress(self):
        """
        Returns the most recent :class:`StreamingQueryProgress` update of this streaming query or
        None if there were no progress updates

        .. versionadded:: 2.1.0

        Returns
        -------
        dict
        """
        lastProgress = self._jsq.lastProgress()
        if lastProgress:
            return json.loads(lastProgress.json())
        else:
            return None

    def processAllAvailable(self):
        """Blocks until all available data in the source has been processed and committed to the
        sink. This method is intended for testing.

        .. versionadded:: 2.0.0

        Notes
        -----
        In the case of continually arriving data, this method may block forever.
        Additionally, this method is only guaranteed to block until data that has been
        synchronously appended data to a stream source prior to invocation.
        (i.e. `getOffset` must immediately reflect the addition).
        """
        return self._jsq.processAllAvailable()

    @since(2.0)
    def stop(self):
        """Stop this streaming query.
        """
        self._jsq.stop()

    def explain(self, extended=False):
        """Prints the (logical and physical) plans to the console for debugging purpose.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('query_explain').start()
        >>> sq.processAllAvailable() # Wait a bit to generate the runtime plans.
        >>> sq.explain()
        == Physical Plan ==
        ...
        >>> sq.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        >>> sq.stop()
        """
        # Cannot call `_jsq.explain(...)` because it will print in the JVM process.
        # We should print it in the Python process.
        print(self._jsq.explainInternal(extended))

    def exception(self):
        """
        .. versionadded:: 2.1.0

        Returns
        -------
        :class:`StreamingQueryException`
            the StreamingQueryException if the query was terminated by an exception, or None.
        """
        if self._jsq.exception().isDefined():
            je = self._jsq.exception().get()
            msg = je.toString().split(': ', 1)[1]  # Drop the Java StreamingQueryException type info
            stackTrace = '\n\t at '.join(map(lambda x: x.toString(), je.getStackTrace()))
            return StreamingQueryException(msg, stackTrace, je.getCause())
        else:
            return None


class StreamingQueryManager(object):
    """A class to manage all the :class:`StreamingQuery` StreamingQueries active.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jsqm):
        self._jsqm = jsqm

    @property
    def active(self):
        """Returns a list of active queries associated with this SQLContext

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sqm = spark.streams
        >>> # get the list of active streaming queries
        >>> [q.name for q in sqm.active]
        ['this_query']
        >>> sq.stop()
        """
        return [StreamingQuery(jsq) for jsq in self._jsqm.active()]

    def get(self, id):
        """Returns an active query from this SQLContext or throws exception if an active query
        with this name doesn't exist.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.name
        'this_query'
        >>> sq = spark.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq = sqlContext.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        return StreamingQuery(self._jsqm.get(id))

    @since(2.0)
    def awaitAnyTermination(self, timeout=None):
        """Wait until any of the queries on the associated SQLContext has terminated since the
        creation of the context, or since :func:`resetTerminated()` was called. If any query was
        terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If a query has terminated, then subsequent calls to :func:`awaitAnyTermination()` will
        either return immediately (if the query was terminated by :func:`query.stop()`),
        or throw the exception immediately (if the query was terminated with exception). Use
        :func:`resetTerminated()` to clear past terminations and wait for new terminations.

        In the case where multiple queries have terminated since :func:`resetTermination()`
        was called, if any query has terminated with exception, then :func:`awaitAnyTermination()`
        will throw any of the exception. For correctly documenting exceptions across multiple
        queries, users need to stop all of them after any of them terminates with exception, and
        then check the `query.exception()` for each query.

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jsqm.awaitAnyTermination(int(timeout * 1000))
        else:
            return self._jsqm.awaitAnyTermination()

    def resetTerminated(self):
        """Forget about past terminated queries so that :func:`awaitAnyTermination()` can be used
        again to wait for new terminations.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.streams.resetTerminated()
        """
        self._jsqm.resetTerminated()


class DataStreamReader(OptionUtils):
    """
    Interface used to load a streaming :class:`DataFrame <pyspark.sql.DataFrame>` from external
    storage systems (e.g. file systems, key-value stores, etc).
    Use :attr:`SparkSession.readStream <pyspark.sql.SparkSession.readStream>` to access this.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, spark):
        self._jreader = spark._ssql_ctx.readStream()
        self._spark = spark

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._spark)

    def format(self, source):
        """Specifies the input data source format.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        source : str
            name of the data source, e.g. 'json', 'parquet'.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> s = spark.readStream.format("text")
        """
        self._jreader = self._jreader.format(source)
        return self

    def schema(self, schema):
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.StructType` or str
            a :class:`pyspark.sql.types.StructType` object or a DDL-formatted string
            (For example ``col0 INT, col1 DOUBLE``).

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> s = spark.readStream.schema(sdf_schema)
        >>> s = spark.readStream.schema("col0 INT, col1 DOUBLE")
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        if isinstance(schema, StructType):
            jschema = spark._jsparkSession.parseDataType(schema.json())
            self._jreader = self._jreader.schema(jschema)
        elif isinstance(schema, str):
            self._jreader = self._jreader.schema(schema)
        else:
            raise TypeError("schema should be StructType or string")
        return self

    def option(self, key, value):
        """Adds an input option for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to parse
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> s = spark.readStream.option("x", 1)
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    def options(self, **options):
        """Adds input options for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to parse
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> s = spark.readStream.options(x="1", y=2)
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    def load(self, path=None, format=None, schema=None, **options):
        """Loads a data stream from a data source and returns it as a
        :class:`DataFrame <pyspark.sql.DataFrame>`.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        path : str, optional
            optional string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source. Default to 'parquet'.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> json_sdf = spark.readStream.format("json") \\
        ...     .schema(sdf_schema) \\
        ...     .load(tempfile.mkdtemp())
        >>> json_sdf.isStreaming
        True
        >>> json_sdf.schema == sdf_schema
        True
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            if type(path) != str or len(path.strip()) == 0:
                raise ValueError("If the path is provided for stream, it needs to be a " +
                                 "non-empty string. List of paths are not supported.")
            return self._df(self._jreader.load(path))
        else:
            return self._df(self._jreader.load())

    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None,  allowUnquotedControlChars=None, lineSep=None, locale=None,
             dropFieldIfAllNull=None, encoding=None, pathGlobFilter=None,
             recursiveFileLookup=None, allowNonNumericNumbers=None):
        """
        Loads a JSON file stream and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        path : str
            string represents path to the JSON dataset,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        primitivesAsString : str or bool, optional
            infers all primitive values as a string type. If None is set,
            it uses the default value, ``false``.
        prefersDecimal : str or bool, optional
            infers all floating-point values as a decimal type. If the values
            do not fit in decimal, then it infers them as doubles. If None is
            set, it uses the default value, ``false``.
        allowComments : str or bool, optional
            ignores Java/C++ style comment in JSON records. If None is set,
            it uses the default value, ``false``.
        allowUnquotedFieldNames : str or bool, optional
            allows unquoted JSON field names. If None is set,
            it uses the default value, ``false``.
        allowSingleQuotes : str or bool, optional
            allows single quotes in addition to double quotes. If None is
            set, it uses the default value, ``true``.
        allowNumericLeadingZero : str or bool, optional
            allows leading zeros in numbers (e.g. 00012). If None is
            set, it uses the default value, ``false``.
        allowBackslashEscapingAnyCharacter : str or bool, optional
            allows accepting quoting of all character
            using backslash quoting mechanism. If None is
            set, it uses the default value, ``false``.
        mode : str, optional
            allows a mode for dealing with corrupt records during parsing. If None is
            set, it uses the default value, ``PERMISSIVE``.

            * ``PERMISSIVE``: when it meets a corrupted record, puts the malformed string \
              into a field configured by ``columnNameOfCorruptRecord``, and sets malformed \
              fields to ``null``. To keep corrupt records, an user can set a string type \
              field named ``columnNameOfCorruptRecord`` in an user-defined schema. If a \
              schema does not have the field, it drops corrupt records during parsing. \
              When inferring a schema, it implicitly adds a ``columnNameOfCorruptRecord`` \
              field in an output schema.
            *  ``DROPMALFORMED``: ignores the whole corrupted records.
            *  ``FAILFAST``: throws an exception when it meets corrupted records.

        columnNameOfCorruptRecord : str, optional
            allows renaming the new field having malformed string
            created by ``PERMISSIVE`` mode. This overrides
            ``spark.sql.columnNameOfCorruptRecord``. If None is set,
            it uses the value specified in
            ``spark.sql.columnNameOfCorruptRecord``.
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats
            follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        multiLine : str or bool, optional
            parse one record, which may span multiple lines, per file. If None is
            set, it uses the default value, ``false``.
        allowUnquotedControlChars : str or bool, optional
            allows JSON Strings to contain unquoted control
            characters (ASCII characters with value less than 32,
            including tab and line feed characters) or not.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
        locale : str, optional
            sets a locale as language tag in IETF BCP 47 format. If None is set,
            it uses the default value, ``en-US``. For instance, ``locale`` is used while
            parsing dates and timestamps.
        dropFieldIfAllNull : str or bool, optional
            whether to ignore column of all null values or empty
            array/struct during schema inference. If None is set, it
            uses the default value, ``false``.
        encoding : str or bool, optional
            allows to forcibly set one of standard basic or extended encoding for
            the JSON files. For example UTF-16BE, UTF-32LE. If None is set,
            the encoding of input JSON will be detected automatically
            when the multiLine option is set to ``true``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        allowNonNumericNumbers : str or bool, optional
            allows JSON parser to recognize set of "Not-a-Number" (NaN)
            tokens as legal floating number values. If None is set,
            it uses the default value, ``true``.

                * ``+INF``: for positive infinity, as well as alias of
                            ``+Infinity`` and ``Infinity``.
                *  ``-INF``: for negative infinity, alias ``-Infinity``.
                *  ``NaN``: for other not-a-numbers, like result of division by zero.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> json_sdf = spark.readStream.json(tempfile.mkdtemp(), schema = sdf_schema)
        >>> json_sdf.isStreaming
        True
        >>> json_sdf.schema == sdf_schema
        True
        """
        self._set_opts(
            schema=schema, primitivesAsString=primitivesAsString, prefersDecimal=prefersDecimal,
            allowComments=allowComments, allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes, allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode, columnNameOfCorruptRecord=columnNameOfCorruptRecord, dateFormat=dateFormat,
            timestampFormat=timestampFormat, multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars, lineSep=lineSep, locale=locale,
            dropFieldIfAllNull=dropFieldIfAllNull, encoding=encoding,
            pathGlobFilter=pathGlobFilter, recursiveFileLookup=recursiveFileLookup,
            allowNonNumericNumbers=allowNonNumericNumbers)
        if isinstance(path, str):
            return self._df(self._jreader.json(path))
        else:
            raise TypeError("path can be only a single string")

    def orc(self, path, mergeSchema=None, pathGlobFilter=None, recursiveFileLookup=None):
        """Loads a ORC file stream, returning the result as a :class:`DataFrame`.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        mergeSchema : str or bool, optional
            sets whether we should merge schemas collected from all
            ORC part-files. This will override ``spark.sql.orc.mergeSchema``.
            The default value is specified in ``spark.sql.orc.mergeSchema``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of `partition discovery`_.
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

        Examples
        --------
        >>> orc_sdf = spark.readStream.schema(sdf_schema).orc(tempfile.mkdtemp())
        >>> orc_sdf.isStreaming
        True
        >>> orc_sdf.schema == sdf_schema
        True
        """
        self._set_opts(mergeSchema=mergeSchema, pathGlobFilter=pathGlobFilter,
                       recursiveFileLookup=recursiveFileLookup)
        if isinstance(path, str):
            return self._df(self._jreader.orc(path))
        else:
            raise TypeError("path can be only a single string")

    def parquet(self, path, mergeSchema=None, pathGlobFilter=None, recursiveFileLookup=None):
        """
        Loads a Parquet file stream, returning the result as a :class:`DataFrame`.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        mergeSchema : str or bool, optional
            sets whether we should merge schemas collected from all
            Parquet part-files. This will override
            ``spark.sql.parquet.mergeSchema``. The default value is specified in
            ``spark.sql.parquet.mergeSchema``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of `partition discovery`_.
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

        Examples
        --------
        >>> parquet_sdf = spark.readStream.schema(sdf_schema).parquet(tempfile.mkdtemp())
        >>> parquet_sdf.isStreaming
        True
        >>> parquet_sdf.schema == sdf_schema
        True
        """
        self._set_opts(mergeSchema=mergeSchema, pathGlobFilter=pathGlobFilter,
                       recursiveFileLookup=recursiveFileLookup)
        if isinstance(path, str):
            return self._df(self._jreader.parquet(path))
        else:
            raise TypeError("path can be only a single string")

    def text(self, path, wholetext=False, lineSep=None, pathGlobFilter=None,
             recursiveFileLookup=None):
        """
        Loads a text file stream and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.
        The text files must be encoded as UTF-8.

        By default, each line in the text file is a new row in the resulting DataFrame.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        paths : str or list
            string, or list of strings, for input path(s).
        wholetext : str or bool, optional
            if true, read each file from input path(s) as a single row.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of `partition discovery`_.
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> text_sdf = spark.readStream.text(tempfile.mkdtemp())
        >>> text_sdf.isStreaming
        True
        >>> "value" in str(text_sdf.schema)
        True
        """
        self._set_opts(
            wholetext=wholetext, lineSep=lineSep, pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup)
        if isinstance(path, str):
            return self._df(self._jreader.text(path))
        else:
            raise TypeError("path can be only a single string")

    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
            maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None,
            columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None,
            enforceSchema=None, emptyValue=None, locale=None, lineSep=None,
            pathGlobFilter=None, recursiveFileLookup=None, unescapedQuoteHandling=None):
        r"""Loads a CSV file stream and returns the result as a :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s).
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        sep : str, optional
            sets a separator (one or more characters) for each field and value. If None is
            set, it uses the default value, ``,``.
        encoding : str, optional
            decodes the CSV files by the given encoding type. If None is set,
            it uses the default value, ``UTF-8``.
        quote : str, optional sets a single character used for escaping quoted values where the
            separator can be part of the value. If None is set, it uses the default
            value, ``"``. If you would like to turn off quotations, you need to set an
            empty string.
        escape : str, optional
            sets a single character used for escaping quotes inside an already
            quoted value. If None is set, it uses the default value, ``\``.
        comment : str, optional
            sets a single character used for skipping lines beginning with this
            character. By default (None), it is disabled.
        header : str or bool, optional
            uses the first line as names of columns. If None is set, it uses the
            default value, ``false``.
        inferSchema : str or bool, optional
            infers the input schema automatically from data. It requires one extra
            pass over the data. If None is set, it uses the default value, ``false``.
        enforceSchema : str or bool, optional
            If it is set to ``true``, the specified or inferred schema will be
            forcibly applied to datasource files, and headers in CSV files will be
            ignored. If the option is set to ``false``, the schema will be
            validated against all headers in CSV files or the first header in RDD
            if the ``header`` option is set to ``true``. Field names in the schema
            and column names in CSV headers are checked by their positions
            taking into account ``spark.sql.caseSensitive``. If None is set,
            ``true`` is used by default. Though the default value is ``true``,
            it is recommended to disable the ``enforceSchema`` option
            to avoid incorrect results.
        ignoreLeadingWhiteSpace : str or bool, optional
            a flag indicating whether or not leading whitespaces from
            values being read should be skipped. If None is set, it
            uses the default value, ``false``.
        ignoreTrailingWhiteSpace : str or bool, optional
            a flag indicating whether or not trailing whitespaces from
            values being read should be skipped. If None is set, it
            uses the default value, ``false``.
        nullValue : str, optional
            sets the string representation of a null value. If None is set, it uses
            the default value, empty string. Since 2.0.1, this ``nullValue`` param
            applies to all supported types including the string type.
        nanValue : str, optional
            sets the string representation of a non-number value. If None is set, it
            uses the default value, ``NaN``.
        positiveInf : str, optional
            sets the string representation of a positive infinity value. If None
            is set, it uses the default value, ``Inf``.
        negativeInf : str, optional
            sets the string representation of a negative infinity value. If None
            is set, it uses the default value, ``Inf``.
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats
            follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        maxColumns : str or int, optional
            defines a hard limit of how many columns a record can have. If None is
            set, it uses the default value, ``20480``.
        maxCharsPerColumn : str or int, optional
            defines the maximum number of characters allowed for any given
            value being read. If None is set, it uses the default value,
            ``-1`` meaning unlimited length.
        maxMalformedLogPerPartition : str or int, optional
            this parameter is no longer used since Spark 2.2.0.
            If specified, it is ignored.
        mode : str, optional
            allows a mode for dealing with corrupt records during parsing. If None is
            set, it uses the default value, ``PERMISSIVE``.

            * ``PERMISSIVE``: when it meets a corrupted record, puts the malformed string \
              into a field configured by ``columnNameOfCorruptRecord``, and sets malformed \
              fields to ``null``. To keep corrupt records, an user can set a string type \
              field named ``columnNameOfCorruptRecord`` in an user-defined schema. If a \
              schema does not have the field, it drops corrupt records during parsing. \
              A record with less/more tokens than schema is not a corrupted record to CSV. \
              When it meets a record having fewer tokens than the length of the schema, \
              sets ``null`` to extra fields. When the record has more tokens than the \
              length of the schema, it drops extra tokens.
            * ``DROPMALFORMED``: ignores the whole corrupted records.
            * ``FAILFAST``: throws an exception when it meets corrupted records.

        columnNameOfCorruptRecord : str, optional
            allows renaming the new field having malformed string
            created by ``PERMISSIVE`` mode. This overrides
            ``spark.sql.columnNameOfCorruptRecord``. If None is set,
            it uses the value specified in
            ``spark.sql.columnNameOfCorruptRecord``.
        multiLine : str or bool, optional
            parse one record, which may span multiple lines. If None is
            set, it uses the default value, ``false``.
        charToEscapeQuoteEscaping : str, optional
            sets a single character used for escaping the escape for
            the quote character. If None is set, the default value is
            escape character when escape and quote characters are
            different, ``\0`` otherwise.
        emptyValue : str, optional
            sets the string representation of an empty value. If None is set, it uses
            the default value, empty string.
        locale : str, optional
            sets a locale as language tag in IETF BCP 47 format. If None is set,
            it uses the default value, ``en-US``. For instance, ``locale`` is used while
            parsing dates and timestamps.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
            Maximum length is 1 character.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        unescapedQuoteHandling : str, optional
            defines how the CsvParser will handle values with unescaped quotes. If None is
            set, it uses the default value, ``STOP_AT_DELIMITER``.

            * ``STOP_AT_CLOSING_QUOTE``: If unescaped quotes are found in the input, accumulate
              the quote character and proceed parsing the value as a quoted value, until a closing
              quote is found.
            * ``BACK_TO_DELIMITER``: If unescaped quotes are found in the input, consider the value
              as an unquoted value. This will make the parser accumulate all characters of the current
              parsed value until the delimiter is found. If no delimiter is found in the value, the
              parser will continue accumulating characters from the input until a delimiter or line
              ending is found.
            * ``STOP_AT_DELIMITER``: If unescaped quotes are found in the input, consider the value
              as an unquoted value. This will make the parser accumulate all characters until the
              delimiter or a line ending is found in the input.
            * ``STOP_AT_DELIMITER``: If unescaped quotes are found in the input, the content parsed
              for the given value will be skipped and the value set in nullValue will be produced
              instead.
            * ``RAISE_ERROR``: If unescaped quotes are found in the input, a TextParsingException
              will be thrown.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> csv_sdf = spark.readStream.csv(tempfile.mkdtemp(), schema = sdf_schema)
        >>> csv_sdf.isStreaming
        True
        >>> csv_sdf.schema == sdf_schema
        True
        """
        self._set_opts(
            schema=schema, sep=sep, encoding=encoding, quote=quote, escape=escape, comment=comment,
            header=header, inferSchema=inferSchema, ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace, nullValue=nullValue,
            nanValue=nanValue, positiveInf=positiveInf, negativeInf=negativeInf,
            dateFormat=dateFormat, timestampFormat=timestampFormat, maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition, mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord, multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping, enforceSchema=enforceSchema,
            emptyValue=emptyValue, locale=locale, lineSep=lineSep,
            pathGlobFilter=pathGlobFilter, recursiveFileLookup=recursiveFileLookup,
            unescapedQuoteHandling=unescapedQuoteHandling)
        if isinstance(path, str):
            return self._df(self._jreader.csv(path))
        else:
            raise TypeError("path can be only a single string")

    def table(self, tableName):
        """Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
        support streaming mode.

        .. versionadded:: 3.2.0

        Parameters
        ----------
        tableName : str
            string, for the name of the table.

        Returns
        --------
        :class:`DataFrame`

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> csv_sdf = spark.readStream.table('input_table') # doctest: +SKIP
        >>> csv_sdf.isStreaming # doctest: +SKIP
        True
        """
        if isinstance(tableName, str):
            return self._df(self._jreader.table(tableName))
        else:
            raise TypeError("tableName can be only a single string")


class DataStreamWriter(object):
    """
    Interface used to write a streaming :class:`DataFrame <pyspark.sql.DataFrame>` to external
    storage systems (e.g. file systems, key-value stores, etc).
    Use :attr:`DataFrame.writeStream <pyspark.sql.DataFrame.writeStream>`
    to access this.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.writeStream()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    def outputMode(self, outputMode):
        """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

        .. versionadded:: 2.0.0

        Options include:

        * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these are some updates
        * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> writer = sdf.writeStream.outputMode('append')
        """
        if not outputMode or type(outputMode) != str or len(outputMode.strip()) == 0:
            raise ValueError('The output mode must be a non-empty string. Got: %s' % outputMode)
        self._jwrite = self._jwrite.outputMode(outputMode)
        return self

    def format(self, source):
        """Specifies the underlying output data source.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        source : str
            string, name of the data source, which for now can be 'parquet'.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> writer = sdf.writeStream.format('json')
        """
        self._jwrite = self._jwrite.format(source)
        return self

    def option(self, key, value):
        """Adds an output option for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to format
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to format
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.

        .. versionadded:: 2.0.0

        Notes
        -----
        This API is evolving.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        cols : str or list
            name of columns

        Notes
        -----
        This API is evolving.
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    def queryName(self, queryName):
        """Specifies the name of the :class:`StreamingQuery` that can be started with
        :func:`start`. This name must be unique among all the currently active queries
        in the associated SparkSession.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        queryName : str
            unique name for the query

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> writer = sdf.writeStream.queryName('streaming_query')
        """
        if not queryName or type(queryName) != str or len(queryName.strip()) == 0:
            raise ValueError('The queryName must be a non-empty string. Got: %s' % queryName)
        self._jwrite = self._jwrite.queryName(queryName)
        return self

    @keyword_only
    def trigger(self, *, processingTime=None, once=None, continuous=None):
        """Set the trigger for the stream query. If this is not set it will run the query as fast
        as possible, which is equivalent to setting the trigger to ``processingTime='0 seconds'``.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        processingTime : str, optional
            a processing time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a microbatch query periodically based on the
            processing time. Only one trigger can be set.
        once : bool, optional
            if set to True, set a trigger that processes only one batch of data in a
            streaming query then terminates the query. Only one trigger can be set.
        continuous : str, optional
            a time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a continuous query with a given checkpoint
            interval. Only one trigger can be set.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> # trigger the query for execution every 5 seconds
        >>> writer = sdf.writeStream.trigger(processingTime='5 seconds')
        >>> # trigger the query for just once batch of data
        >>> writer = sdf.writeStream.trigger(once=True)
        >>> # trigger the query for execution every 5 seconds
        >>> writer = sdf.writeStream.trigger(continuous='5 seconds')
        """
        params = [processingTime, once, continuous]

        if params.count(None) == 3:
            raise ValueError('No trigger provided')
        elif params.count(None) < 2:
            raise ValueError('Multiple triggers not allowed.')

        jTrigger = None
        if processingTime is not None:
            if type(processingTime) != str or len(processingTime.strip()) == 0:
                raise ValueError('Value for processingTime must be a non empty string. Got: %s' %
                                 processingTime)
            interval = processingTime.strip()
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.ProcessingTime(
                interval)

        elif once is not None:
            if once is not True:
                raise ValueError('Value for once must be True. Got: %s' % once)
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.Once()

        else:
            if type(continuous) != str or len(continuous.strip()) == 0:
                raise ValueError('Value for continuous must be a non empty string. Got: %s' %
                                 continuous)
            interval = continuous.strip()
            jTrigger = self._spark._sc._jvm.org.apache.spark.sql.streaming.Trigger.Continuous(
                interval)

        self._jwrite = self._jwrite.trigger(jTrigger)
        return self

    def foreach(self, f):
        """
        Sets the output of the streaming query to be processed using the provided writer ``f``.
        This is often used to write the output of a streaming query to arbitrary storage systems.
        The processing logic can be specified in two ways.

        #. A **function** that takes a row as input.
            This is a simple way to express your processing logic. Note that this does
            not allow you to deduplicate generated data when failures cause reprocessing of
            some input data. That would require you to specify the processing logic in the next
            way.

        #. An **object** with a ``process`` method and optional ``open`` and ``close`` methods.
            The object can have the following methods.

            * ``open(partition_id, epoch_id)``: *Optional* method that initializes the processing
                (for example, open a connection, start a transaction, etc). Additionally, you can
                use the `partition_id` and `epoch_id` to deduplicate regenerated data
                (discussed later).

            * ``process(row)``: *Non-optional* method that processes each :class:`Row`.

            * ``close(error)``: *Optional* method that finalizes and cleans up (for example,
                close connection, commit transaction, etc.) after all rows have been processed.

            The object will be used by Spark in the following way.

            * A single copy of this object is responsible of all the data generated by a
                single task in a query. In other words, one instance is responsible for
                processing one partition of the data generated in a distributed manner.

            * This object must be serializable because each task will get a fresh
                serialized-deserialized copy of the provided object. Hence, it is strongly
                recommended that any initialization for writing data (e.g. opening a
                connection or starting a transaction) is done after the `open(...)`
                method has been called, which signifies that the task is ready to generate data.

            * The lifecycle of the methods are as follows.

                For each partition with ``partition_id``:

                ... For each batch/epoch of streaming data with ``epoch_id``:

                ....... Method ``open(partitionId, epochId)`` is called.

                ....... If ``open(...)`` returns true, for each row in the partition and
                        batch/epoch, method ``process(row)`` is called.

                ....... Method ``close(errorOrNull)`` is called with error (if any) seen while
                        processing rows.

            Important points to note:

            * The `partitionId` and `epochId` can be used to deduplicate generated data when
                failures cause reprocessing of some input data. This depends on the execution
                mode of the query. If the streaming query is being executed in the micro-batch
                mode, then every partition represented by a unique tuple (partition_id, epoch_id)
                is guaranteed to have the same data. Hence, (partition_id, epoch_id) can be used
                to deduplicate and/or transactionally commit data and achieve exactly-once
                guarantees. However, if the streaming query is being executed in the continuous
                mode, then this guarantee does not hold and therefore should not be used for
                deduplication.

            * The ``close()`` method (if exists) will be called if `open()` method exists and
                returns successfully (irrespective of the return value), except if the Python
                crashes in the middle.

        .. versionadded:: 2.4.0

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> # Print every row using a function
        >>> def print_row(row):
        ...     print(row)
        ...
        >>> writer = sdf.writeStream.foreach(print_row)
        >>> # Print every row using a object with process() method
        >>> class RowPrinter:
        ...     def open(self, partition_id, epoch_id):
        ...         print("Opened %d, %d" % (partition_id, epoch_id))
        ...         return True
        ...     def process(self, row):
        ...         print(row)
        ...     def close(self, error):
        ...         print("Closed with error: %s" % str(error))
        ...
        >>> writer = sdf.writeStream.foreach(RowPrinter())
        """

        from pyspark.rdd import _wrap_function
        from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
        from pyspark.taskcontext import TaskContext

        if callable(f):
            # The provided object is a callable function that is supposed to be called on each row.
            # Construct a function that takes an iterator and calls the provided function on each
            # row.
            def func_without_process(_, iterator):
                for x in iterator:
                    f(x)
                return iter([])

            func = func_without_process

        else:
            # The provided object is not a callable function. Then it is expected to have a
            # 'process(row)' method, and optional 'open(partition_id, epoch_id)' and
            # 'close(error)' methods.

            if not hasattr(f, 'process'):
                raise Exception("Provided object does not have a 'process' method")

            if not callable(getattr(f, 'process')):
                raise Exception("Attribute 'process' in provided object is not callable")

            def doesMethodExist(method_name):
                exists = hasattr(f, method_name)
                if exists and not callable(getattr(f, method_name)):
                    raise Exception(
                        "Attribute '%s' in provided object is not callable" % method_name)
                return exists

            open_exists = doesMethodExist('open')
            close_exists = doesMethodExist('close')

            def func_with_open_process_close(partition_id, iterator):
                epoch_id = TaskContext.get().getLocalProperty('streaming.sql.batchId')
                if epoch_id:
                    epoch_id = int(epoch_id)
                else:
                    raise Exception("Could not get batch id from TaskContext")

                # Check if the data should be processed
                should_process = True
                if open_exists:
                    should_process = f.open(partition_id, epoch_id)

                error = None

                try:
                    if should_process:
                        for x in iterator:
                            f.process(x)
                except Exception as ex:
                    error = ex
                finally:
                    if close_exists:
                        f.close(error)
                    if error:
                        raise error

                return iter([])

            func = func_with_open_process_close

        serializer = AutoBatchedSerializer(PickleSerializer())
        wrapped_func = _wrap_function(self._spark._sc, func, serializer, serializer)
        jForeachWriter = \
            self._spark._sc._jvm.org.apache.spark.sql.execution.python.PythonForeachWriter(
                wrapped_func, self._df._jdf.schema())
        self._jwrite.foreach(jForeachWriter)
        return self

    def foreachBatch(self, func):
        """
        Sets the output of the streaming query to be processed using the provided
        function. This is supported only the in the micro-batch execution modes (that is, when the
        trigger is not continuous). In every micro-batch, the provided function will be called in
        every micro-batch with (i) the output rows as a DataFrame and (ii) the batch identifier.
        The batchId can be used deduplicate and transactionally write the output
        (that is, the provided Dataset) to external systems. The output DataFrame is guaranteed
        to exactly same for the same batchId (assuming all operations are deterministic in the
        query).

        .. versionadded:: 2.4.0

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> def func(batch_df, batch_id):
        ...     batch_df.collect()
        ...
        >>> writer = sdf.writeStream.foreachBatch(func)
        """

        from pyspark.java_gateway import ensure_callback_server_started
        gw = self._spark._sc._gateway
        java_import(gw.jvm, "org.apache.spark.sql.execution.streaming.sources.*")

        wrapped_func = ForeachBatchFunction(self._spark, func)
        gw.jvm.PythonForeachBatchHelper.callForeachBatch(self._jwrite, wrapped_func)
        ensure_callback_server_started(gw)
        return self

    def start(self, path=None, format=None, outputMode=None, partitionBy=None, queryName=None,
              **options):
        """Streams the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        path : str, optional
            the path in a Hadoop supported file system
        format : str, optional
            the format used to save
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.

            * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to the
              sink
            * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the
              sink every time these are some updates
            * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
              written to the sink every time there are some updates. If the query doesn't contain
              aggregations, it will be equivalent to `append` mode.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`
            for most streams, however it is not required for a `memory` stream.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.isActive
        True
        >>> sq.name
        'this_query'
        >>> sq.stop()
        >>> sq.isActive
        False
        >>> sq = sdf.writeStream.trigger(processingTime='5 seconds').start(
        ...     queryName='that_query', outputMode="append", format='memory')
        >>> sq.name
        'that_query'
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)
        if path is None:
            return self._sq(self._jwrite.start())
        else:
            return self._sq(self._jwrite.start(path))

    def toTable(self, tableName, format=None, outputMode=None, partitionBy=None, queryName=None,
                **options):
        """
        Starts the execution of the streaming query, which will continually output results to the given
        table as new data arrives.

        A new table will be created if the table not exists. The returned
        :class:`StreamingQuery` object can be used to interact with the stream.

        .. versionadded:: 3.2.0

        Parameters
        ----------
        tableName : str
            string, for the name of the table.
        format : str, optional
            the format used to save.
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.

            * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to the
              sink
            * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the
              sink every time these are some updates
            * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
              written to the sink every time there are some updates. If the query doesn't contain
              aggregations, it will be equivalent to `append` mode.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> sq = sdf.writeStream.format('parquet').queryName('this_query').option(
        ...      'checkpointLocation', '/tmp/checkpoint').toTable('output_table') # doctest: +SKIP
        >>> sq.isActive # doctest: +SKIP
        True
        >>> sq.name # doctest: +SKIP
        'this_query'
        >>> sq.stop() # doctest: +SKIP
        >>> sq.isActive # doctest: +SKIP
        False
        >>> sq = sdf.writeStream.trigger(processingTime='5 seconds').toTable(
        ...     'output_table', queryName='that_query', outputMode="append", format='parquet',
        ...     checkpointLocation='/tmp/checkpoint') # doctest: +SKIP
        >>> sq.name # doctest: +SKIP
        'that_query'
        >>> sq.isActive # doctest: +SKIP
        True
        >>> sq.stop() # doctest: +SKIP
        """
        # TODO(SPARK-33659): document the current behavior for DataStreamWriter.toTable API
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)
        return self._sq(self._jwrite.toTable(tableName))


def _test():
    import doctest
    import os
    import tempfile
    from random import randint
    from pyspark.sql import SparkSession, SQLContext
    import pyspark.sql.streaming

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.__dict__.copy()
    try:
        spark = SparkSession.builder.getOrCreate()
    except py4j.protocol.Py4JError:  # noqa: F821
        spark = SparkSession(sc)  # noqa: F821

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['spark'] = spark
    globs['sqlContext'] = SQLContext.getOrCreate(spark.sparkContext)
    globs['sdf'] = \
        spark.readStream.format('text').load('python/test_support/sql/streaming')
    globs['sdf_schema'] = StructType([StructField("data", StringType(), True)])
    globs['df'] = \
        globs['spark'].readStream.format('text').load('python/test_support/sql/streaming')

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.streaming, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['spark'].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
