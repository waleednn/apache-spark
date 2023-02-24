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

check_dependencies(__name__, __file__)

from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    Sequence,
    Tuple,
    Optional,
)

import json
import decimal
import datetime
import warnings
from threading import Lock

import numpy as np

from pyspark.sql.types import (
    _from_numpy_type,
    DateType,
    NullType,
    BooleanType,
    BinaryType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    DataType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
)

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.types import (
    JVM_BYTE_MIN,
    JVM_BYTE_MAX,
    JVM_SHORT_MIN,
    JVM_SHORT_MAX,
    JVM_INT_MIN,
    JVM_INT_MAX,
    JVM_LONG_MIN,
    JVM_LONG_MAX,
    pyspark_types_to_proto_types,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.window import WindowSpec


class Expression:
    """
    Expression base class.
    """

    def __init__(self) -> None:
        pass

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        ...

    def __repr__(self) -> str:
        ...

    def alias(self, *alias: str, **kwargs: Any) -> "ColumnAlias":
        metadata = kwargs.pop("metadata", None)
        assert not kwargs, "Unexpected kwargs where passed: %s" % kwargs
        return ColumnAlias(self, list(alias), metadata)

    def name(self) -> str:
        ...


class CaseWhen(Expression):
    def __init__(
        self, branches: Sequence[Tuple[Expression, Expression]], else_value: Optional[Expression]
    ):

        assert isinstance(branches, list)
        for branch in branches:
            assert (
                isinstance(branch, tuple)
                and len(branch) == 2
                and all(isinstance(expr, Expression) for expr in branch)
            )
        self._branches = branches

        if else_value is not None:
            assert isinstance(else_value, Expression)

        self._else_value = else_value

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        args = []
        for condition, value in self._branches:
            args.append(condition)
            args.append(value)

        if self._else_value is not None:
            args.append(self._else_value)

        unresolved_function = UnresolvedFunction(name="when", args=args)

        return unresolved_function.to_plan(session)

    def __repr__(self) -> str:
        _cases = "".join([f" WHEN {c} THEN {v}" for c, v in self._branches])
        _else = f" ELSE {self._else_value}" if self._else_value is not None else ""
        return "CASE" + _cases + _else + " END"


class ColumnAlias(Expression):
    def __init__(self, parent: Expression, alias: Sequence[str], metadata: Any):

        self._alias = alias
        self._metadata = metadata
        self._parent = parent

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        if len(self._alias) == 1:
            exp = proto.Expression()
            exp.alias.name.append(self._alias[0])
            exp.alias.expr.CopyFrom(self._parent.to_plan(session))

            if self._metadata:
                exp.alias.metadata = json.dumps(self._metadata)
            return exp
        else:
            if self._metadata:
                raise ValueError("metadata can only be provided for a single column")
            exp = proto.Expression()
            exp.alias.name.extend(self._alias)
            exp.alias.expr.CopyFrom(self._parent.to_plan(session))
            return exp

    def __repr__(self) -> str:
        return f"{self._parent} AS {','.join(self._alias)}"


class LiteralExpression(Expression):
    """A literal expression.

    The Python types are converted best effort into the relevant proto types. On the Spark Connect
    server side, the proto types are converted to the Catalyst equivalents."""

    def __init__(self, value: Any, dataType: DataType) -> None:
        super().__init__()

        assert isinstance(
            dataType,
            (
                NullType,
                BinaryType,
                BooleanType,
                ByteType,
                ShortType,
                IntegerType,
                LongType,
                FloatType,
                DoubleType,
                DecimalType,
                StringType,
                DateType,
                TimestampType,
                TimestampNTZType,
                DayTimeIntervalType,
            ),
        )

        if isinstance(dataType, NullType):
            assert value is None

        if value is not None:
            if isinstance(dataType, BinaryType):
                assert isinstance(value, (bytes, bytearray))
            elif isinstance(dataType, BooleanType):
                assert isinstance(value, (bool, np.bool_))
                value = bool(value)
            elif isinstance(dataType, ByteType):
                assert isinstance(value, (int, np.int8))
                assert JVM_BYTE_MIN <= int(value) <= JVM_BYTE_MAX
                value = int(value)
            elif isinstance(dataType, ShortType):
                assert isinstance(value, (int, np.int8, np.int16))
                assert JVM_SHORT_MIN <= int(value) <= JVM_SHORT_MAX
                value = int(value)
            elif isinstance(dataType, IntegerType):
                assert isinstance(value, (int, np.int8, np.int16, np.int32))
                assert JVM_INT_MIN <= int(value) <= JVM_INT_MAX
                value = int(value)
            elif isinstance(dataType, LongType):
                assert isinstance(value, (int, np.int8, np.int16, np.int32, np.int64))
                assert JVM_LONG_MIN <= int(value) <= JVM_LONG_MAX
                value = int(value)
            elif isinstance(dataType, FloatType):
                assert isinstance(value, (float, np.float32))
                value = float(value)
            elif isinstance(dataType, DoubleType):
                assert isinstance(value, (float, np.float32, np.float64))
                value = float(value)
            elif isinstance(dataType, DecimalType):
                assert isinstance(value, decimal.Decimal)
            elif isinstance(dataType, StringType):
                assert isinstance(value, str)
            elif isinstance(dataType, DateType):
                assert isinstance(value, (datetime.date, datetime.datetime))
                if isinstance(value, datetime.date):
                    value = DateType().toInternal(value)
                else:
                    value = DateType().toInternal(value.date())
            elif isinstance(dataType, TimestampType):
                assert isinstance(value, datetime.datetime)
                value = TimestampType().toInternal(value)
            elif isinstance(dataType, TimestampNTZType):
                assert isinstance(value, datetime.datetime)
                value = TimestampNTZType().toInternal(value)
            elif isinstance(dataType, DayTimeIntervalType):
                assert isinstance(value, datetime.timedelta)
                value = DayTimeIntervalType().toInternal(value)
                assert value is not None
            else:
                raise TypeError(f"Unsupported Data Type {dataType}")

        self._value = value
        self._dataType = dataType

    @classmethod
    def _infer_type(cls, value: Any) -> DataType:
        if value is None:
            return NullType()
        elif isinstance(value, (bytes, bytearray)):
            return BinaryType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            if JVM_INT_MIN <= value <= JVM_INT_MAX:
                return IntegerType()
            elif JVM_LONG_MIN <= value <= JVM_LONG_MAX:
                return LongType()
            else:
                raise ValueError(f"integer {value} out of bounds")
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, decimal.Decimal):
            return DecimalType()
        elif isinstance(value, datetime.datetime):
            return TimestampType()
        elif isinstance(value, datetime.date):
            return DateType()
        elif isinstance(value, datetime.timedelta):
            return DayTimeIntervalType()
        else:
            if isinstance(value, np.generic):
                dt = _from_numpy_type(value.dtype)
                if dt is not None:
                    return dt
                elif isinstance(value, np.bool_):
                    return BooleanType()
            raise TypeError(f"Unsupported Data Type {type(value).__name__}")

    @classmethod
    def _from_value(cls, value: Any) -> "LiteralExpression":
        return LiteralExpression(value=value, dataType=LiteralExpression._infer_type(value))

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        """Converts the literal expression to the literal in proto."""

        expr = proto.Expression()

        if self._value is None:
            expr.literal.null.CopyFrom(pyspark_types_to_proto_types(self._dataType))
        elif isinstance(self._dataType, BinaryType):
            expr.literal.binary = bytes(self._value)
        elif isinstance(self._dataType, BooleanType):
            expr.literal.boolean = bool(self._value)
        elif isinstance(self._dataType, ByteType):
            expr.literal.byte = int(self._value)
        elif isinstance(self._dataType, ShortType):
            expr.literal.short = int(self._value)
        elif isinstance(self._dataType, IntegerType):
            expr.literal.integer = int(self._value)
        elif isinstance(self._dataType, LongType):
            expr.literal.long = int(self._value)
        elif isinstance(self._dataType, FloatType):
            expr.literal.float = float(self._value)
        elif isinstance(self._dataType, DoubleType):
            expr.literal.double = float(self._value)
        elif isinstance(self._dataType, DecimalType):
            expr.literal.decimal.value = str(self._value)
            expr.literal.decimal.precision = self._dataType.precision
            expr.literal.decimal.scale = self._dataType.scale
        elif isinstance(self._dataType, StringType):
            expr.literal.string = str(self._value)
        elif isinstance(self._dataType, DateType):
            expr.literal.date = int(self._value)
        elif isinstance(self._dataType, TimestampType):
            expr.literal.timestamp = int(self._value)
        elif isinstance(self._dataType, TimestampNTZType):
            expr.literal.timestamp_ntz = int(self._value)
        elif isinstance(self._dataType, DayTimeIntervalType):
            expr.literal.day_time_interval = int(self._value)
        else:
            raise ValueError(f"Unsupported Data Type {self._dataType}")

        return expr

    def __repr__(self) -> str:
        return f"{self._value}"


class ColumnReference(Expression):
    """Represents a column reference. There is no guarantee that this column
    actually exists. In the context of this project, we refer by its name and
    treat it as an unresolved attribute. Attributes that have the same fully
    qualified name are identical"""

    def __init__(self, unparsed_identifier: str, plan_id: Optional[int] = None) -> None:
        super().__init__()
        assert isinstance(unparsed_identifier, str)
        self._unparsed_identifier = unparsed_identifier

        assert plan_id is None or isinstance(plan_id, int)
        self._plan_id = plan_id

    def name(self) -> str:
        """Returns the qualified name of the column reference."""
        return self._unparsed_identifier

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the expression."""
        expr = proto.Expression()
        expr.unresolved_attribute.unparsed_identifier = self._unparsed_identifier
        if self._plan_id is not None:
            expr.unresolved_attribute.plan_id = self._plan_id
        return expr

    def __repr__(self) -> str:
        return f"{self._unparsed_identifier}"

    def __eq__(self, other: Any) -> bool:
        return (
            other is not None
            and isinstance(other, ColumnReference)
            and other._unparsed_identifier == self._unparsed_identifier
        )


class UnresolvedStar(Expression):
    def __init__(self, unparsed_target: Optional[str]):
        super().__init__()

        if unparsed_target is not None:
            assert isinstance(unparsed_target, str) and unparsed_target.endswith(".*")

        self._unparsed_target = unparsed_target

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = proto.Expression()
        expr.unresolved_star.SetInParent()
        if self._unparsed_target is not None:
            expr.unresolved_star.unparsed_target = self._unparsed_target
        return expr

    def __repr__(self) -> str:
        if self._unparsed_target is not None:
            return f"unresolvedstar({self._unparsed_target})"
        else:
            return "unresolvedstar()"

    def __eq__(self, other: Any) -> bool:
        return (
            other is not None
            and isinstance(other, UnresolvedStar)
            and other._unparsed_target == self._unparsed_target
        )


class SQLExpression(Expression):
    """Returns Expression which contains a string which is a SQL expression
    and server side will parse it by Catalyst
    """

    def __init__(self, expr: str) -> None:
        super().__init__()
        self._expr: str = expr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        """Returns the Proto representation of the SQL expression."""
        expr = proto.Expression()
        expr.expression_string.expression = self._expr
        return expr

    def __eq__(self, other: Any) -> bool:
        return other is not None and isinstance(other, SQLExpression) and other._expr == self._expr


class SortOrder(Expression):
    def __init__(self, child: Expression, ascending: bool = True, nullsFirst: bool = True) -> None:
        super().__init__()
        self._child = child
        self._ascending = ascending
        self._nullsFirst = nullsFirst

    def __repr__(self) -> str:
        return (
            str(self._child)
            + (" ASC" if self._ascending else " DESC")
            + (" NULLS FIRST" if self._nullsFirst else " NULLS LAST")
        )

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        sort = proto.Expression()
        sort.sort_order.child.CopyFrom(self._child.to_plan(session))

        if self._ascending:
            sort.sort_order.direction = (
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
            )
        else:
            sort.sort_order.direction = (
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
            )

        if self._nullsFirst:
            sort.sort_order.null_ordering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        else:
            sort.sort_order.null_ordering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST

        return sort


class UnresolvedFunction(Expression):
    def __init__(
        self,
        name: str,
        args: Sequence["Expression"],
        is_distinct: bool = False,
    ) -> None:
        super().__init__()

        assert isinstance(name, str)
        self._name = name

        assert isinstance(args, list) and all(isinstance(arg, Expression) for arg in args)
        self._args = args

        assert isinstance(is_distinct, bool)
        self._is_distinct = is_distinct

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.unresolved_function.function_name = self._name
        if len(self._args) > 0:
            fun.unresolved_function.arguments.extend([arg.to_plan(session) for arg in self._args])
        fun.unresolved_function.is_distinct = self._is_distinct
        return fun

    def __repr__(self) -> str:
        # Default print handling:
        if self._is_distinct:
            return f"{self._name}(distinct {', '.join([str(arg) for arg in self._args])})"
        else:
            return f"{self._name}({', '.join([str(arg) for arg in self._args])})"


class PythonUDF:
    """Represents a Python user-defined function."""

    def __init__(
        self,
        output_type: str,
        eval_type: int,
        command: bytes,
        python_ver: str,
    ) -> None:
        self._output_type = output_type
        self._eval_type = eval_type
        self._command = command
        self._python_ver = python_ver

    def to_plan(self, session: "SparkConnectClient") -> proto.PythonUDF:
        expr = proto.PythonUDF()
        expr.output_type = self._output_type
        expr.eval_type = self._eval_type
        expr.command = self._command
        expr.python_ver = self._python_ver
        return expr

    def __repr__(self) -> str:
        return (
            f"{self._output_type}, {self._eval_type}, "
            f"{self._command}, f{self._python_ver}"  # type: ignore[str-bytes-safe]
        )


class JavaUDF:
    """Represents a Java user-defined function."""

    def __init__(
        self,
        class_name: str,
        output_type: str,
    ) -> None:
        self._class_name = class_name
        self._output_type = output_type

    def to_plan(self, session: "SparkConnectClient") -> proto.JavaUDF:
        expr = proto.JavaUDF()
        expr.class_name = self._class_name
        expr.output_type = self._output_type
        return expr

    def __repr__(self) -> str:
        return f"{self._class_name}, {self._output_type}"


class CommonInlineUserDefinedFunction(Expression):
    """Represents a user-defined function with an inlined defined function body of any programming
    languages."""

    def __init__(
        self,
        function_name: str,
        deterministic: bool,
        arguments: Sequence[Expression],
        function: Union[PythonUDF, JavaUDF],
    ):
        self._function_name = function_name
        self._deterministic = deterministic
        self._arguments = arguments
        self._function = function

    def to_plan(self, session: "SparkConnectClient") -> "proto.Expression":
        expr = proto.Expression()
        expr.common_inline_user_defined_function.function_name = self._function_name
        expr.common_inline_user_defined_function.deterministic = self._deterministic
        if len(self._arguments) > 0:
            expr.common_inline_user_defined_function.arguments.extend(
                [arg.to_plan(session) for arg in self._arguments]
            )
        expr.common_inline_user_defined_function.python_udf.CopyFrom(
            self._function.to_plan(session)
        )
        return expr

    def to_plan_udf(self, session: "SparkConnectClient") -> "proto.CommonInlineUserDefinedFunction":
        """Compared to `to_plan`, it returns a CommonInlineUserDefinedFunction instead of an
        Expression."""
        expr = proto.CommonInlineUserDefinedFunction()
        expr.function_name = self._function_name
        expr.deterministic = self._deterministic
        if len(self._arguments) > 0:
            expr.arguments.extend([arg.to_plan(session) for arg in self._arguments])
        expr.python_udf.CopyFrom(self._function.to_plan(session))
        return expr

    def to_plan_judf(
        self, session: "SparkConnectClient"
    ) -> "proto.CommonInlineUserDefinedFunction":
        expr = proto.CommonInlineUserDefinedFunction()
        expr.function_name = self._function_name
        expr.java_udf.CopyFrom(self._function.to_plan(session))
        return expr

    def __repr__(self) -> str:
        return f"{self._function_name}({', '.join([str(arg) for arg in self._arguments])})"


class WithField(Expression):
    def __init__(
        self,
        structExpr: Expression,
        fieldName: str,
        valueExpr: Expression,
    ) -> None:
        super().__init__()

        assert isinstance(structExpr, Expression)
        self._structExpr = structExpr

        assert isinstance(fieldName, str)
        self._fieldName = fieldName

        assert isinstance(valueExpr, Expression)
        self._valueExpr = valueExpr

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.update_fields.struct_expression.CopyFrom(self._structExpr.to_plan(session))
        expr.update_fields.field_name = self._fieldName
        expr.update_fields.value_expression.CopyFrom(self._valueExpr.to_plan(session))
        return expr

    def __repr__(self) -> str:
        return f"WithField({self._structExpr}, {self._fieldName}, {self._valueExpr})"


class DropField(Expression):
    def __init__(
        self,
        structExpr: Expression,
        fieldName: str,
    ) -> None:
        super().__init__()

        assert isinstance(structExpr, Expression)
        self._structExpr = structExpr

        assert isinstance(fieldName, str)
        self._fieldName = fieldName

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.update_fields.struct_expression.CopyFrom(self._structExpr.to_plan(session))
        expr.update_fields.field_name = self._fieldName
        return expr

    def __repr__(self) -> str:
        return f"DropField({self._structExpr}, {self._fieldName})"


class UnresolvedExtractValue(Expression):
    def __init__(
        self,
        child: Expression,
        extraction: Expression,
    ) -> None:
        super().__init__()

        assert isinstance(child, Expression)
        self._child = child

        assert isinstance(extraction, Expression)
        self._extraction = extraction

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.unresolved_extract_value.child.CopyFrom(self._child.to_plan(session))
        expr.unresolved_extract_value.extraction.CopyFrom(self._extraction.to_plan(session))
        return expr

    def __repr__(self) -> str:
        return f"UnresolvedExtractValue({str(self._child)}, {str(self._extraction)})"


class UnresolvedRegex(Expression):
    def __init__(self, col_name: str, plan_id: Optional[int] = None) -> None:
        super().__init__()

        assert isinstance(col_name, str)
        self.col_name = col_name

        assert plan_id is None or isinstance(plan_id, int)
        self._plan_id = plan_id

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.unresolved_regex.col_name = self.col_name
        if self._plan_id is not None:
            expr.unresolved_regex.plan_id = self._plan_id
        return expr

    def __repr__(self) -> str:
        return f"UnresolvedRegex({self.col_name})"


class CastExpression(Expression):
    def __init__(
        self,
        expr: Expression,
        data_type: Union[DataType, str],
    ) -> None:
        super().__init__()
        self._expr = expr
        self._data_type = data_type

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        fun = proto.Expression()
        fun.cast.expr.CopyFrom(self._expr.to_plan(session))
        if isinstance(self._data_type, str):
            fun.cast.type_str = self._data_type
        else:
            fun.cast.type.CopyFrom(pyspark_types_to_proto_types(self._data_type))
        return fun

    def __repr__(self) -> str:
        return f"({self._expr} ({self._data_type}))"


class UnresolvedNamedLambdaVariable(Expression):

    _lock: Lock = Lock()
    _nextVarNameId: int = 0

    def __init__(
        self,
        name_parts: Sequence[str],
    ) -> None:
        super().__init__()

        assert (
            isinstance(name_parts, list)
            and len(name_parts) > 0
            and all(isinstance(p, str) for p in name_parts)
        )

        self._name_parts = name_parts

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.unresolved_named_lambda_variable.name_parts.extend(self._name_parts)
        return expr

    def __repr__(self) -> str:
        return f"(UnresolvedNamedLambdaVariable({', '.join(self._name_parts)})"

    @staticmethod
    def fresh_var_name(name: str) -> str:
        assert isinstance(name, str) and str != ""

        _id: Optional[int] = None

        with UnresolvedNamedLambdaVariable._lock:
            _id = UnresolvedNamedLambdaVariable._nextVarNameId
            UnresolvedNamedLambdaVariable._nextVarNameId += 1

        assert _id is not None

        return f"{name}_{_id}"


class LambdaFunction(Expression):
    def __init__(
        self,
        function: Expression,
        arguments: Sequence[UnresolvedNamedLambdaVariable],
    ) -> None:
        super().__init__()

        assert isinstance(function, Expression)

        assert (
            isinstance(arguments, list)
            and len(arguments) > 0
            and all(isinstance(arg, UnresolvedNamedLambdaVariable) for arg in arguments)
        )

        self._function = function
        self._arguments = arguments

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()
        expr.lambda_function.function.CopyFrom(self._function.to_plan(session))
        expr.lambda_function.arguments.extend(
            [arg.to_plan(session).unresolved_named_lambda_variable for arg in self._arguments]
        )
        return expr

    def __repr__(self) -> str:
        return f"(LambdaFunction({str(self._function)}, {', '.join(self._arguments)})"


class WindowExpression(Expression):
    def __init__(
        self,
        windowFunction: Expression,
        windowSpec: "WindowSpec",
    ) -> None:
        super().__init__()

        from pyspark.sql.connect.window import WindowSpec

        assert windowFunction is not None and isinstance(windowFunction, Expression)

        assert windowSpec is not None and isinstance(windowSpec, WindowSpec)

        self._windowFunction = windowFunction

        self._windowSpec = windowSpec

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        expr = proto.Expression()

        expr.window.window_function.CopyFrom(self._windowFunction.to_plan(session))

        if len(self._windowSpec._partitionSpec) > 0:
            expr.window.partition_spec.extend(
                [p.to_plan(session) for p in self._windowSpec._partitionSpec]
            )
        else:
            warnings.warn(
                "WARN WindowExpression: No Partition Defined for Window operation! "
                "Moving all data to a single partition, this can cause serious "
                "performance degradation."
            )

        if len(self._windowSpec._orderSpec) > 0:
            expr.window.order_spec.extend(
                [s.to_plan(session).sort_order for s in self._windowSpec._orderSpec]
            )

        if self._windowSpec._frame is not None:
            if self._windowSpec._frame._isRowFrame:
                expr.window.frame_spec.frame_type = (
                    proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
                )

                start = self._windowSpec._frame._start
                if start == 0:
                    expr.window.frame_spec.lower.current_row = True
                elif start == JVM_LONG_MIN:
                    expr.window.frame_spec.lower.unbounded = True
                elif JVM_INT_MIN <= start <= JVM_INT_MAX:
                    expr.window.frame_spec.lower.value.literal.integer = start
                else:
                    raise ValueError(f"start is out of bound: {start}")

                end = self._windowSpec._frame._end
                if end == 0:
                    expr.window.frame_spec.upper.current_row = True
                elif end == JVM_LONG_MAX:
                    expr.window.frame_spec.upper.unbounded = True
                elif JVM_INT_MIN <= end <= JVM_INT_MAX:
                    expr.window.frame_spec.upper.value.literal.integer = end
                else:
                    raise ValueError(f"end is out of bound: {end}")

            else:
                expr.window.frame_spec.frame_type = (
                    proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
                )

                start = self._windowSpec._frame._start
                if start == 0:
                    expr.window.frame_spec.lower.current_row = True
                elif start == JVM_LONG_MIN:
                    expr.window.frame_spec.lower.unbounded = True
                else:
                    expr.window.frame_spec.lower.value.literal.long = start

                end = self._windowSpec._frame._end
                if end == 0:
                    expr.window.frame_spec.upper.current_row = True
                elif end == JVM_LONG_MAX:
                    expr.window.frame_spec.upper.unbounded = True
                else:
                    expr.window.frame_spec.upper.value.literal.long = end

        return expr

    def __repr__(self) -> str:
        return f"WindowExpression({str(self._windowFunction)}, ({str(self._windowSpec)}))"
