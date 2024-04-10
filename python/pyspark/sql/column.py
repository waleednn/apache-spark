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
import warnings
import inspect
from typing import (
    cast,
    overload,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from pyspark.errors import PySparkAttributeError, PySparkTypeError, PySparkValueError
from pyspark.sql.types import DataType
from pyspark.sql.utils import get_active_spark_context

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.core.context import SparkContext
    from pyspark.sql._typing import ColumnOrName, LiteralType, DecimalLiteral, DateTimeLiteral
    from pyspark.sql.window import WindowSpec

__all__ = ["Column"]


def _create_column_from_literal(literal: Union["LiteralType", "DecimalLiteral"]) -> "Column":
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    return cast(JVMView, sc._jvm).functions.lit(literal)


def _create_column_from_name(name: str) -> "Column":
    from py4j.java_gateway import JVMView

    sc = get_active_spark_context()
    return cast(JVMView, sc._jvm).functions.col(name)


def _to_java_column(col: "ColumnOrName") -> "JavaObject":
    if isinstance(col, Column):
        jcol = col._jc
    elif isinstance(col, str):
        jcol = _create_column_from_name(col)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
        )
    return jcol


def _to_java_expr(col: "ColumnOrName") -> "JavaObject":
    return _to_java_column(col).expr()


@overload
def _to_seq(sc: "SparkContext", cols: Iterable["JavaObject"]) -> "JavaObject":
    ...


@overload
def _to_seq(
    sc: "SparkContext",
    cols: Iterable["ColumnOrName"],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]],
) -> "JavaObject":
    ...


def _to_seq(
    sc: "SparkContext",
    cols: Union[Iterable["ColumnOrName"], Iterable["JavaObject"]],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]] = None,
) -> "JavaObject":
    """
    Convert a list of Columns (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    assert sc._jvm is not None
    return sc._jvm.PythonUtils.toSeq(cols)


def _to_list(
    sc: "SparkContext",
    cols: List["ColumnOrName"],
    converter: Optional[Callable[["ColumnOrName"], "JavaObject"]] = None,
) -> "JavaObject":
    """
    Convert a list of Columns (or names) into a JVM (Scala) List of Columns.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    assert sc._jvm is not None
    return sc._jvm.PythonUtils.toList(cols)


def _unary_op(
    name: str,
    doc: str = "unary operator",
) -> Callable[["Column"], "Column"]:
    """Create a method for given unary operator"""

    def _(self: "Column") -> "Column":
        jc = getattr(self._jc, name)()
        return Column(jc)

    _.__doc__ = doc
    return _


def _func_op(name: str, doc: str = "") -> Callable[["Column"], "Column"]:
    def _(self: "Column") -> "Column":
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()
        jc = getattr(cast(JVMView, sc._jvm).functions, name)(self._jc)
        return Column(jc)

    _.__doc__ = doc
    return _


def _bin_func_op(
    name: str,
    reverse: bool = False,
    doc: str = "binary function",
) -> Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"]:
    def _(self: "Column", other: Union["Column", "LiteralType", "DecimalLiteral"]) -> "Column":
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()
        fn = getattr(cast(JVMView, sc._jvm).functions, name)
        jc = other._jc if isinstance(other, Column) else _create_column_from_literal(other)
        njc = fn(self._jc, jc) if not reverse else fn(jc, self._jc)
        return Column(njc)

    _.__doc__ = doc
    return _


def _bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[
    ["Column", Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]], "Column"
]:
    """Create a method for given binary operator"""
    binary_operator_map = {
        "plus": "+",
        "minus": "-",
        "divide": "/",
        "multiply": "*",
        "mod": "%",
        "equalTo": "=",
        "lt": "<",
        "leq": "<=",
        "geq": ">=",
        "gt": ">",
        "eqNullSafe": "<=>",
        "bitwiseOR": "|",
        "bitwiseAND": "&",
        "bitwiseXOR": "^",
        # Just following JVM rule even if the names of source and target are the same.
        "and": "and",
        "or": "or",
    }

    def _(
        self: "Column",
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        jc = other._jc if isinstance(other, Column) else other
        if name in binary_operator_map:
            from pyspark.sql import SparkSession

            spark = SparkSession._getActiveSessionOrCreate()
            stack = list(reversed(inspect.stack()))
            depth = int(
                spark.conf.get("spark.sql.stackTracesInDataFrameContext")  # type: ignore[arg-type]
            )
            selected_frames = stack[:depth]
            logging_info_list = [f"{frame.filename}:{frame.lineno}" for frame in selected_frames]
            logging_info_str = "\n".join(logging_info_list)
            logging_info = (name, logging_info_str)

            njc = getattr(self._jc, "fn")(binary_operator_map[name], jc, logging_info)
        else:
            njc = getattr(self._jc, name)(jc)
        return Column(njc)

    _.__doc__ = doc
    _.__name__ = name
    return _


def _reverse_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[["Column", Union["LiteralType", "DecimalLiteral"]], "Column"]:
    """Create a method for binary operator (this object is on right side)"""

    def _(self: "Column", other: Union["LiteralType", "DecimalLiteral"]) -> "Column":
        jother = _create_column_from_literal(other)
        jc = getattr(jother, name)(self._jc)
        return Column(jc)

    _.__doc__ = doc
    return _


class Column:

    """
    A column in a DataFrame.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    Column instances can be created by

    >>> df = spark.createDataFrame(
    ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])

    Select a column out of a DataFrame
    >>> df.name
    Column<'name'>
    >>> df["name"]
    Column<'name'>

    Create from an expression

    >>> df.age + 1
    Column<...>
    >>> 1 / df.age
    Column<...>
    """

    def __init__(self, jc: "JavaObject") -> None:
        self._jc = jc

    # arithmetic operators
    __neg__ = _func_op("negate")
    __add__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("plus"),
    )
    __sub__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("minus"),
    )
    __mul__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("multiply"),
    )
    __div__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("divide"),
    )
    __truediv__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("divide"),
    )
    __mod__ = cast(
        Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral"]], "Column"],
        _bin_op("mod"),
    )
    __radd__ = cast(
        Callable[["Column", Union["LiteralType", "DecimalLiteral"]], "Column"], _bin_op("plus")
    )
    __rsub__ = _reverse_op("minus")
    __rmul__ = cast(
        Callable[["Column", Union["LiteralType", "DecimalLiteral"]], "Column"], _bin_op("multiply")
    )
    __rdiv__ = _reverse_op("divide")
    __rtruediv__ = _reverse_op("divide")
    __rmod__ = _reverse_op("mod")

    __pow__ = _bin_func_op("pow")
    __rpow__ = cast(
        Callable[["Column", Union["LiteralType", "DecimalLiteral"]], "Column"],
        _bin_func_op("pow", reverse=True),
    )

    # logistic operators
    def __eq__(  # type: ignore[override]
        self,
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        """binary function"""
        return _bin_op("equalTo")(self, other)

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return _bin_op("notEqual")(self, other)

    __lt__ = _bin_op("lt")
    __le__ = _bin_op("leq")
    __ge__ = _bin_op("geq")
    __gt__ = _bin_op("gt")

    _eqNullSafe_doc = """
    Equality test that is safe for null values.

    .. versionadded:: 2.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other
        a value or :class:`Column`

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df1 = spark.createDataFrame([
    ...     Row(id=1, value='foo'),
    ...     Row(id=2, value=None)
    ... ])
    >>> df1.select(
    ...     df1['value'] == 'foo',
    ...     df1['value'].eqNullSafe('foo'),
    ...     df1['value'].eqNullSafe(None)
    ... ).show()
    +-------------+---------------+----------------+
    |(value = foo)|(value <=> foo)|(value <=> NULL)|
    +-------------+---------------+----------------+
    |         true|           true|           false|
    |         NULL|          false|            true|
    +-------------+---------------+----------------+
    >>> df2 = spark.createDataFrame([
    ...     Row(value = 'bar'),
    ...     Row(value = None)
    ... ])
    >>> df1.join(df2, df1["value"] == df2["value"]).count()
    0
    >>> df1.join(df2, df1["value"].eqNullSafe(df2["value"])).count()
    1
    >>> df2 = spark.createDataFrame([
    ...     Row(id=1, value=float('NaN')),
    ...     Row(id=2, value=42.0),
    ...     Row(id=3, value=None)
    ... ])
    >>> df2.select(
    ...     df2['value'].eqNullSafe(None),
    ...     df2['value'].eqNullSafe(float('NaN')),
    ...     df2['value'].eqNullSafe(42.0)
    ... ).show()
    +----------------+---------------+----------------+
    |(value <=> NULL)|(value <=> NaN)|(value <=> 42.0)|
    +----------------+---------------+----------------+
    |           false|           true|           false|
    |           false|          false|            true|
    |            true|          false|           false|
    +----------------+---------------+----------------+

    Notes
    -----
    Unlike Pandas, PySpark doesn't consider NaN values to be NULL. See the
    `NaN Semantics <https://spark.apache.org/docs/latest/sql-ref-datatypes.html#nan-semantics>`_
    for details.
    """
    eqNullSafe = _bin_op("eqNullSafe", _eqNullSafe_doc)

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op("and")
    __or__ = _bin_op("or")
    __invert__ = _func_op("not")
    __rand__ = _bin_op("and")
    __ror__ = _bin_op("or")

    # container operators
    def __contains__(self, item: Any) -> None:
        raise PySparkValueError(
            error_class="CANNOT_APPLY_IN_FOR_COLUMN",
            message_parameters={},
        )

    # bitwise operators
    _bitwiseOR_doc = """
    Compute bitwise OR of this expression with another expression.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other
        a value or :class:`Column` to calculate bitwise or(|) with
        this :class:`Column`.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseOR(df.b)).collect()
    [Row((a | b)=235)]
    """
    _bitwiseAND_doc = """
    Compute bitwise AND of this expression with another expression.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other
        a value or :class:`Column` to calculate bitwise and(&) with
        this :class:`Column`.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseAND(df.b)).collect()
    [Row((a & b)=10)]
    """
    _bitwiseXOR_doc = """
    Compute bitwise XOR of this expression with another expression.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other
        a value or :class:`Column` to calculate bitwise xor(^) with
        this :class:`Column`.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(a=170, b=75)])
    >>> df.select(df.a.bitwiseXOR(df.b)).collect()
    [Row((a ^ b)=225)]
    """

    bitwiseOR = _bin_op("bitwiseOR", _bitwiseOR_doc)
    bitwiseAND = _bin_op("bitwiseAND", _bitwiseAND_doc)
    bitwiseXOR = _bin_op("bitwiseXOR", _bitwiseXOR_doc)

    def getItem(self, key: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key
            a literal value, or a :class:`Column` expression.
            The result will only be true at a location if the item matches in the column.

             .. deprecated:: 3.0.0
                 :class:`Column` as a parameter is deprecated.

        Returns
        -------
        :class:`Column`
            Column representing the item(s) got at position out of a list or by key out of a dict.

        Examples
        --------
        >>> df = spark.createDataFrame([([1, 2], {"key": "value"})], ["l", "d"])
        >>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
        +----+------+
        |l[0]|d[key]|
        +----+------+
        |   1| value|
        +----+------+
        """
        if isinstance(key, Column):
            warnings.warn(
                "A column as 'key' in getItem is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[key]` or `column.key` syntax "
                "instead.",
                FutureWarning,
            )
        return self[key]

    def getField(self, name: Any) -> "Column":
        """
        An expression that gets a field by name in a :class:`StructType`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name
            a literal value, or a :class:`Column` expression.
            The result will only be true at a location if the field matches in the Column.

             .. deprecated:: 3.0.0
                 :class:`Column` as a parameter is deprecated.
        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column got by name.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(r=Row(a=1, b="b"))])
        >>> df.select(df.r.getField("b")).show()
        +---+
        |r.b|
        +---+
        |  b|
        +---+
        >>> df.select(df.r.a).show()
        +---+
        |r.a|
        +---+
        |  1|
        +---+
        """
        if isinstance(name, Column):
            warnings.warn(
                "A column as 'name' in getField is deprecated as of Spark 3.0, and will not "
                "be supported in the future release. Use `column[name]` or `column.name` syntax "
                "instead.",
                FutureWarning,
            )
        return self[name]

    def withField(self, fieldName: str, col: "Column") -> "Column":
        """
        An expression that adds/replaces a field in :class:`StructType` by name.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        fieldName : str
            a literal value.
            The result will only be true at a location if any field matches in the Column.
        col : :class:`Column`
            A :class:`Column` expression for the column with `fieldName`.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column
            which field was added/replaced by fieldName.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import lit
        >>> df = spark.createDataFrame([Row(a=Row(b=1, c=2))])
        >>> df.withColumn('a', df['a'].withField('b', lit(3))).select('a.b').show()
        +---+
        |  b|
        +---+
        |  3|
        +---+
        >>> df.withColumn('a', df['a'].withField('d', lit(4))).select('a.d').show()
        +---+
        |  d|
        +---+
        |  4|
        +---+
        """
        if not isinstance(fieldName, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "fieldName", "arg_type": type(fieldName).__name__},
            )

        if not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )

        return Column(self._jc.withField(fieldName, col._jc))

    def dropFields(self, *fieldNames: str) -> "Column":
        """
        An expression that drops fields in :class:`StructType` by name.
        This is a no-op if the schema doesn't contain field name(s).

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        fieldNames : str
            Desired field names (collects all positional arguments passed)
            The result will drop at a location if any field matches in the Column.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column with field dropped by fieldName.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import col, lit
        >>> df = spark.createDataFrame([
        ...     Row(a=Row(b=1, c=2, d=3, e=Row(f=4, g=5, h=6)))])
        >>> df.withColumn('a', df['a'].dropFields('b')).show()
        +-----------------+
        |                a|
        +-----------------+
        |{2, 3, {4, 5, 6}}|
        +-----------------+

        >>> df.withColumn('a', df['a'].dropFields('b', 'c')).show()
        +--------------+
        |             a|
        +--------------+
        |{3, {4, 5, 6}}|
        +--------------+

        This method supports dropping multiple nested fields directly e.g.

        >>> df.withColumn("a", col("a").dropFields("e.g", "e.h")).show()
        +--------------+
        |             a|
        +--------------+
        |{1, 2, 3, {4}}|
        +--------------+

        However, if you are going to add/replace multiple nested fields,
        it is preferred to extract out the nested struct before
        adding/replacing multiple fields e.g.

        >>> df.select(col("a").withField(
        ...     "e", col("a.e").dropFields("g", "h")).alias("a")
        ... ).show()
        +--------------+
        |             a|
        +--------------+
        |{1, 2, 3, {4}}|
        +--------------+

        """
        sc = get_active_spark_context()
        jc = self._jc.dropFields(_to_seq(sc, fieldNames))
        return Column(jc)

    def __getattr__(self, item: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        item
            a literal value.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.d.key).show()
        +------+
        |d[key]|
        +------+
        | value|
        +------+
        """
        if item.startswith("__"):
            raise PySparkAttributeError(
                error_class="CANNOT_ACCESS_TO_DUNDER",
                message_parameters={},
            )
        return self[item]

    def __getitem__(self, k: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        k
            a literal value, or a slice object without step.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict, or substrings sliced by
            the given slice object.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.l[slice(1, 3)], df.d['key']).show()
        +---------------+------+
        |substr(l, 1, 3)|d[key]|
        +---------------+------+
        |            abc| value|
        +---------------+------+
        """
        if isinstance(k, slice):
            if k.step is not None:
                raise PySparkValueError(
                    error_class="SLICE_WITH_STEP",
                    message_parameters={},
                )
            return self.substr(k.start, k.stop)
        else:
            return _bin_op("apply")(self, k)

    def __iter__(self) -> None:
        raise PySparkTypeError(
            error_class="NOT_ITERABLE", message_parameters={"objectName": "Column"}
        )

    # string methods
    _contains_doc = """
    Contains the other element. Returns a boolean :class:`Column` based on a string match.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other
        string in line. A value as a literal or a :class:`Column`.

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
    >>> df.filter(df.name.contains('o')).collect()
    [Row(age=5, name='Bob')]
    """
    _startswith_doc = """
    String starts with. Returns a boolean :class:`Column` based on a string match.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other : :class:`Column` or str
        string at start of line (do not use a regex `^`)

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
    >>> df.filter(df.name.startswith('Al')).collect()
    [Row(age=2, name='Alice')]
    >>> df.filter(df.name.startswith('^Al')).collect()
    []
    """
    _endswith_doc = """
    String ends with. Returns a boolean :class:`Column` based on a string match.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    other : :class:`Column` or str
        string at end of line (do not use a regex `$`)

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
    >>> df.filter(df.name.endswith('ice')).collect()
    [Row(age=2, name='Alice')]
    >>> df.filter(df.name.endswith('ice$')).collect()
    []
    """

    contains = _bin_op("contains", _contains_doc)
    startswith = _bin_op("startsWith", _startswith_doc)
    endswith = _bin_op("endsWith", _endswith_doc)

    def like(self: "Column", other: str) -> "Column":
        """
        SQL like expression. Returns a boolean :class:`Column` based on a SQL LIKE match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            a SQL LIKE pattern

        See Also
        --------
        pyspark.sql.Column.rlike

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by SQL LIKE pattern.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.like('Al%')).collect()
        [Row(age=2, name='Alice')]
        """
        njc = getattr(self._jc, "like")(other)
        return Column(njc)

    def rlike(self: "Column", other: str) -> "Column":
        """
        SQL RLIKE expression (LIKE with Regex). Returns a boolean :class:`Column` based on a regex
        match.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            an extended regex expression

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by extended regex expression.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.rlike('ice$')).collect()
        [Row(age=2, name='Alice')]
        """
        njc = getattr(self._jc, "rlike")(other)
        return Column(njc)

    def ilike(self: "Column", other: str) -> "Column":
        """
        SQL ILIKE expression (case insensitive LIKE). Returns a boolean :class:`Column`
        based on a case insensitive match.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : str
            a SQL LIKE pattern

        See Also
        --------
        pyspark.sql.Column.rlike

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element
            in the Column is matched by SQL LIKE pattern.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.filter(df.name.ilike('%Ice')).collect()
        [Row(age=2, name='Alice')]
        """
        njc = getattr(self._jc, "ilike")(other)
        return Column(njc)

    @overload
    def substr(self, startPos: int, length: int) -> "Column":
        ...

    @overload
    def substr(self, startPos: "Column", length: "Column") -> "Column":
        ...

    def substr(self, startPos: Union[int, "Column"], length: Union[int, "Column"]) -> "Column":
        """
        Return a :class:`Column` which is a substring of the column.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        startPos : :class:`Column` or int
            start position
        length : :class:`Column` or int
            length of the substring

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is substr of origin Column.

        Examples
        --------

        Example 1. Using integers for the input arguments.

        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name.substr(1, 3).alias("col")).collect()
        [Row(col='Ali'), Row(col='Bob')]

        Example 2. Using columns for the input arguments.

        >>> df = spark.createDataFrame(
        ...      [(3, 4, "Alice"), (2, 3, "Bob")], ["sidx", "eidx", "name"])
        >>> df.select(df.name.substr(df.sidx, df.eidx).alias("col")).collect()
        [Row(col='ice'), Row(col='ob')]
        """
        if type(startPos) != type(length):
            raise PySparkTypeError(
                error_class="NOT_SAME_TYPE",
                message_parameters={
                    "arg_name1": "startPos",
                    "arg_name2": "length",
                    "arg_type1": type(startPos).__name__,
                    "arg_type2": type(length).__name__,
                },
            )
        if isinstance(startPos, int):
            jc = self._jc.substr(startPos, length)
        elif isinstance(startPos, Column):
            jc = self._jc.substr(startPos._jc, cast("Column", length)._jc)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_INT",
                message_parameters={"arg_name": "startPos", "arg_type": type(startPos).__name__},
            )
        return Column(jc)

    def isin(self, *cols: Any) -> "Column":
        """
        A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : Any
            The values to compare with the column values. The result will only be true at a location
            if any value matches in the Column.

        Returns
        -------
        :class:`Column`
            Column of booleans showing whether each element in the Column is contained in cols.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob"), (8, "Mike")], ["age", "name"])

        Example 1: Filter rows with names in the specified values

        >>> df[df.name.isin("Bob", "Mike")].show()
        +---+----+
        |age|name|
        +---+----+
        |  5| Bob|
        |  8|Mike|
        +---+----+

        Example 2: Filter rows with ages in the specified list

        >>> df[df.age.isin([1, 2, 3])].show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        +---+-----+

        Example 3: Filter rows with names not in the specified values

        >>> df[~df.name.isin("Alice", "Bob")].show()
        +---+----+
        |age|name|
        +---+----+
        |  8|Mike|
        +---+----+
        """
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cast(Tuple, cols[0])
        cols = cast(
            Tuple,
            [c._jc if isinstance(c, Column) else _create_column_from_literal(c) for c in cols],
        )
        sc = get_active_spark_context()
        jc = getattr(self._jc, "isin")(_to_seq(sc, cols))
        return Column(jc)

    # order
    _asc_doc = """
    Returns a sort expression based on the ascending order of the column.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc()).collect()
    [Row(name='Alice'), Row(name='Tom')]
    """
    _asc_nulls_first_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    return before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_first()).collect()
    [Row(name=None), Row(name='Alice'), Row(name='Tom')]

    """
    _asc_nulls_last_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_last()).collect()
    [Row(name='Alice'), Row(name='Tom'), Row(name=None)]

    """
    _desc_doc = """
    Returns a sort expression based on the descending order of the column.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc()).collect()
    [Row(name='Tom'), Row(name='Alice')]
    """
    _desc_nulls_first_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_first()).collect()
    [Row(name=None), Row(name='Tom'), Row(name='Alice')]

    """
    _desc_nulls_last_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_last()).collect()
    [Row(name='Tom'), Row(name='Alice'), Row(name=None)]
    """

    asc = _unary_op("asc", _asc_doc)
    asc_nulls_first = _unary_op("asc_nulls_first", _asc_nulls_first_doc)
    asc_nulls_last = _unary_op("asc_nulls_last", _asc_nulls_last_doc)
    desc = _unary_op("desc", _desc_doc)
    desc_nulls_first = _unary_op("desc_nulls_first", _desc_nulls_first_doc)
    desc_nulls_last = _unary_op("desc_nulls_last", _desc_nulls_last_doc)

    _isNull_doc = """
    True if the current expression is null.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(name='Tom', height=80), Row(name='Alice', height=None)])
    >>> df.filter(df.height.isNull()).collect()
    [Row(name='Alice', height=None)]
    """
    _isNotNull_doc = """
    True if the current expression is NOT null.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(name='Tom', height=80), Row(name='Alice', height=None)])
    >>> df.filter(df.height.isNotNull()).collect()
    [Row(name='Tom', height=80)]
    """
    _isNaN_doc = """
    True if the current expression is NaN.

    .. versionadded:: 4.0.0

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame(
    ...     [Row(name='Tom', height=80.0), Row(name='Alice', height=float('nan'))])
    >>> df.filter(df.height.isNaN()).collect()
    [Row(name='Alice', height=nan)]
    """

    isNull = _unary_op("isNull", _isNull_doc)
    isNotNull = _unary_op("isNotNull", _isNotNull_doc)
    isNaN = _unary_op("isNaN", _isNaN_doc)

    def alias(self, *alias: str, **kwargs: Any) -> "Column":
        """
        Returns this column aliased with a new name or names (in the case of expressions that
        return more than one column, such as explode).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        alias : str
            desired column names (collects all positional arguments passed)

        Other Parameters
        ----------------
        metadata: dict
            a dict of information to be stored in ``metadata`` attribute of the
            corresponding :class:`StructField <pyspark.sql.types.StructField>` (optional, keyword
            only argument)

            .. versionchanged:: 2.2.0
               Added optional ``metadata`` argument.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is aliased with new name or names.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.age.alias("age2")).collect()
        [Row(age2=2), Row(age2=5)]
        >>> df.select(df.age.alias("age3", metadata={'max': 99})).schema['age3'].metadata['max']
        99
        """

        metadata = kwargs.pop("metadata", None)
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs

        sc = get_active_spark_context()
        if len(alias) == 1:
            if metadata:
                assert sc._jvm is not None
                jmeta = sc._jvm.org.apache.spark.sql.types.Metadata.fromJson(json.dumps(metadata))
                return Column(getattr(self._jc, "as")(alias[0], jmeta))
            else:
                return Column(getattr(self._jc, "as")(alias[0]))
        else:
            if metadata is not None:
                raise PySparkValueError(
                    error_class="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
                    message_parameters={"arg_name": "metadata"},
                )
            return Column(getattr(self._jc, "as")(_to_seq(sc, list(alias))))

    def name(self, *alias: str, **kwargs: Any) -> "Column":
        """
        :func:`name` is an alias for :func:`alias`.

        .. versionadded:: 2.0.0
        """
        return self.alias(*alias, **kwargs)

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        """
        Casts the column into type ``dataType``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        dataType : :class:`DataType` or str
            a DataType or Python string literal with a DDL-formatted string
            to use when parsing the column to the same type.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is cast into new type.

        Examples
        --------
        >>> from pyspark.sql.types import StringType
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.age.cast("string").alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        >>> df.select(df.age.cast(StringType()).alias('ages')).collect()
        [Row(ages='2'), Row(ages='5')]
        """
        if isinstance(dataType, str):
            jc = self._jc.cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SparkSession

            spark = SparkSession._getActiveSessionOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
            jc = self._jc.cast(jdt)
        else:
            raise PySparkTypeError(
                error_class="NOT_DATATYPE_OR_STR",
                message_parameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )
        return Column(jc)

    def try_cast(self, dataType: Union[DataType, str]) -> "Column":
        """
        This is a special version of `cast` that performs the same operation, but returns a NULL
        value instead of raising an error if the invoke method throws exception.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        dataType : :class:`DataType` or str
            a DataType or Python string literal with a DDL-formatted string
            to use when parsing the column to the same type.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is cast into new type.

        Examples
        --------
        Example 1: Cast with a Datatype

        >>> from pyspark.sql.types import LongType
        >>> df = spark.createDataFrame(
        ...      [(2, "123"), (5, "Bob"), (3, None)], ["age", "name"])
        >>> df.select(df.name.try_cast(LongType())).show()
        +----+
        |name|
        +----+
        | 123|
        |NULL|
        |NULL|
        +----+

        Example 2: Cast with a DDL string

        >>> df = spark.createDataFrame(
        ...      [(2, "123"), (5, "Bob"), (3, None)], ["age", "name"])
        >>> df.select(df.name.try_cast("double")).show()
        +-----+
        | name|
        +-----+
        |123.0|
        | NULL|
        | NULL|
        +-----+
        """
        if isinstance(dataType, str):
            jc = self._jc.try_cast(dataType)
        elif isinstance(dataType, DataType):
            from pyspark.sql import SparkSession

            spark = SparkSession._getActiveSessionOrCreate()
            jdt = spark._jsparkSession.parseDataType(dataType.json())
            jc = self._jc.try_cast(jdt)
        else:
            raise PySparkTypeError(
                error_class="NOT_DATATYPE_OR_STR",
                message_parameters={"arg_name": "dataType", "arg_type": type(dataType).__name__},
            )
        return Column(jc)

    def astype(self, dataType: Union[DataType, str]) -> "Column":
        """
        :func:`astype` is an alias for :func:`cast`.

        .. versionadded:: 1.4.0
        """
        return self.cast(dataType)

    def between(
        self,
        lowerBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
        upperBound: Union["Column", "LiteralType", "DateTimeLiteral", "DecimalLiteral"],
    ) -> "Column":
        """
        Check if the current column's values are between the specified lower and upper
        bounds, inclusive.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        lowerBound : :class:`Column`, int, float, string, bool, datetime, date or Decimal
            The lower boundary value, inclusive.
        upperBound : :class:`Column`, int, float, string, bool, datetime, date or Decimal
            The upper boundary value, inclusive.

        Returns
        -------
        :class:`Column`
            A new column of boolean values indicating whether each element in the original
            column is within the specified range (inclusive).

        Examples
        --------
        Using between with integer values.

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name, df.age.between(2, 4)).show()
        +-----+---------------------------+
        | name|((age >= 2) AND (age <= 4))|
        +-----+---------------------------+
        |Alice|                       true|
        |  Bob|                      false|
        +-----+---------------------------+

        Using between with string values.

        >>> df = spark.createDataFrame([("Alice", "A"), ("Bob", "B")], ["name", "initial"])
        >>> df.select(df.name, df.initial.between("A", "B")).show()
        +-----+-----------------------------------+
        | name|((initial >= A) AND (initial <= B))|
        +-----+-----------------------------------+
        |Alice|                               true|
        |  Bob|                               true|
        +-----+-----------------------------------+

        Using between with float values.

        >>> df = spark.createDataFrame(
        ...     [(2.5, "Alice"), (5.5, "Bob")], ["height", "name"])
        >>> df.select(df.name, df.height.between(2.0, 5.0)).show()
        +-----+-------------------------------------+
        | name|((height >= 2.0) AND (height <= 5.0))|
        +-----+-------------------------------------+
        |Alice|                                 true|
        |  Bob|                                false|
        +-----+-------------------------------------+

        Using between with date values.

        >>> import pyspark.sql.functions as sf
        >>> df = spark.createDataFrame(
        ...     [("Alice", "2023-01-01"), ("Bob", "2023-02-01")], ["name", "date"])
        >>> df = df.withColumn("date", sf.to_date(df.date))
        >>> df.select(df.name, df.date.between("2023-01-01", "2023-01-15")).show()
        +-----+-----------------------------------------------+
        | name|((date >= 2023-01-01) AND (date <= 2023-01-15))|
        +-----+-----------------------------------------------+
        |Alice|                                           true|
        |  Bob|                                          false|
        +-----+-----------------------------------------------+
        >>> from datetime import date
        >>> df.select(df.name, df.date.between(date(2023, 1, 1), date(2023, 1, 15))).show()
        +-----+-------------------------------------------------------------+
        | name|((date >= DATE '2023-01-01') AND (date <= DATE '2023-01-15'))|
        +-----+-------------------------------------------------------------+
        |Alice|                                                         true|
        |  Bob|                                                        false|
        +-----+-------------------------------------------------------------+

        Using between with timestamp values.

        >>> import pyspark.sql.functions as sf
        >>> df = spark.createDataFrame(
        ...     [("Alice", "2023-01-01 10:00:00"), ("Bob", "2023-02-01 10:00:00")],
        ...     schema=["name", "timestamp"])
        >>> df = df.withColumn("timestamp", sf.to_timestamp(df.timestamp))
        >>> df.select(df.name, df.timestamp.between("2023-01-01", "2023-02-01")).show()
        +-----+---------------------------------------------------------+
        | name|((timestamp >= 2023-01-01) AND (timestamp <= 2023-02-01))|
        +-----+---------------------------------------------------------+
        |Alice|                                                     true|
        |  Bob|                                                    false|
        +-----+---------------------------------------------------------+
        >>> df.select(df.name, df.timestamp.between("2023-01-01", "2023-02-01 12:00:00")).show()
        +-----+------------------------------------------------------------------+
        | name|((timestamp >= 2023-01-01) AND (timestamp <= 2023-02-01 12:00:00))|
        +-----+------------------------------------------------------------------+
        |Alice|                                                              true|
        |  Bob|                                                              true|
        +-----+------------------------------------------------------------------+
        """
        return (self >= lowerBound) & (self <= upperBound)

    def when(self, condition: "Column", value: Any) -> "Column":
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        condition : :class:`Column`
            a boolean :class:`Column` expression.
        value
            a literal value, or a :class:`Column` expression.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is in conditions.

        Examples
        --------
        Example 1: Using :func:`when` with conditions and values to create a new Column

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> result = df.select(df.name, sf.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0))
        >>> result.show()
        +-----+------------------------------------------------------------+
        | name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0 END|
        +-----+------------------------------------------------------------+
        |Alice|                                                          -1|
        |  Bob|                                                           1|
        +-----+------------------------------------------------------------+

        Example 2: Chaining multiple :func:`when` conditions

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(1, "Alice"), (4, "Bob"), (6, "Charlie")], ["age", "name"])
        >>> result = df.select(
        ...     df.name,
        ...     sf.when(df.age < 3, "Young").when(df.age < 5, "Middle-aged").otherwise("Old")
        ... )
        >>> result.show()
        +-------+---------------------------------------------------------------------------+
        |   name|CASE WHEN (age < 3) THEN Young WHEN (age < 5) THEN Middle-aged ELSE Old END|
        +-------+---------------------------------------------------------------------------+
        |  Alice|                                                                      Young|
        |    Bob|                                                                Middle-aged|
        |Charlie|                                                                        Old|
        +-------+---------------------------------------------------------------------------+

        Example 3: Using literal values as conditions

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> result = df.select(
        ...     df.name, sf.when(sf.lit(True), 1).otherwise(
        ...         sf.raise_error("unreachable")).alias("when"))
        >>> result.show()
        +-----+----+
        | name|when|
        +-----+----+
        |Alice|   1|
        |  Bob|   1|
        +-----+----+

        See Also
        --------
        pyspark.sql.functions.when
        """
        if not isinstance(condition, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.when(condition._jc, v)
        return Column(jc)

    def otherwise(self, value: Any) -> "Column":
        """
        Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        value
            a literal value, or a :class:`Column` expression.

        Returns
        -------
        :class:`Column`
            Column representing whether each element of Column is unmatched conditions.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.select(df.name, sf.when(df.age > 3, 1).otherwise(0)).show()
        +-----+-------------------------------------+
        | name|CASE WHEN (age > 3) THEN 1 ELSE 0 END|
        +-----+-------------------------------------+
        |Alice|                                    0|
        |  Bob|                                    1|
        +-----+-------------------------------------+

        See Also
        --------
        pyspark.sql.functions.when
        """
        v = value._jc if isinstance(value, Column) else value
        jc = self._jc.otherwise(v)
        return Column(jc)

    def over(self, window: "WindowSpec") -> "Column":
        """
        Define a windowing column.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        window : :class:`WindowSpec`

        Returns
        -------
        :class:`Column`

        Examples
        --------
        >>> from pyspark.sql import Window
        >>> window = (
        ...     Window.partitionBy("name")
        ...     .orderBy("age")
        ...     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ... )
        >>> from pyspark.sql.functions import rank, min, desc
        >>> df = spark.createDataFrame(
        ...      [(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df.withColumn(
        ...      "rank", rank().over(window)
        ... ).withColumn(
        ...      "min", min('age').over(window)
        ... ).sort(desc("age")).show()
        +---+-----+----+---+
        |age| name|rank|min|
        +---+-----+----+---+
        |  5|  Bob|   1|  5|
        |  2|Alice|   1|  2|
        +---+-----+----+---+
        """
        from pyspark.sql.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                error_class="NOT_WINDOWSPEC",
                message_parameters={"arg_name": "window", "arg_type": type(window).__name__},
            )
        jc = self._jc.over(window._jspec)
        return Column(jc)

    def __nonzero__(self) -> None:
        raise PySparkValueError(
            error_class="CANNOT_CONVERT_COLUMN_INTO_BOOL",
            message_parameters={},
        )

    __bool__ = __nonzero__

    def __repr__(self) -> str:
        return "Column<'%s'>" % self._jc.toString()


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.column

    globs = pyspark.sql.column.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.column tests").getOrCreate()
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.column,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
