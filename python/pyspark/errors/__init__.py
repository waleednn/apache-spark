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

from typing import Dict, Optional, Union, Any, Type
from pyspark.errors.error_classes import ERROR_CLASSES


class PySparkException(Exception):
    """
    Base Exception for handling the errors generated by PySpark
    """

    def __init__(self, error_class: str, message_parameters: Optional[Dict[str, str]] = None):
        self._verify_error_class(error_class)
        self._error_class = error_class

        self._error_message_format = ERROR_CLASSES[error_class]

        self._verify_message_parameters(message_parameters)
        self._message_parameters = message_parameters

    def _verify_error_class(self, error_class: str) -> None:
        assert (
            error_class in ERROR_CLASSES
        ), f"{error_class} is not in the list of error classes: {list(ERROR_CLASSES.keys())}"

    def _verify_message_parameters(
        self, message_parameters: Optional[Dict[str, str]] = None
    ) -> None:
        required = set(self._error_message_format.__code__.co_varnames)
        given = set() if message_parameters is None else set(message_parameters.keys())
        assert given == required, f"Given message parameters: {given} , but {required} required"

    def getErrorClass(self) -> str:
        return self._error_class

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        return self._message_parameters

    def getErrorMessage(self) -> str:
        if self._message_parameters is None:
            message = self._error_message_format()  # type: ignore[operator]
        else:
            message = self._error_message_format(
                *self._message_parameters.values()
            )  # type: ignore[operator]

        return message

    def __str__(self) -> str:
        # The user-facing error message is contains error class and error message
        # e.g. "[WRONG_NUM_COLUMNS] 'greatest' should take at least two columns"
        return f"[{self.getErrorClass()}] {self.getErrorMessage()}"


def notColumnOrStringError(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    return PySparkException(
        error_class="NOT_COLUMN_OR_STRING",
        message_parameters={"arg_name": arg_name, "arg_type": arg_type.__name__},
    )


def notColumnOrIntegerError(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    return PySparkException(
        error_class="NOT_COLUMN_OR_INTEGER",
        message_parameters={"arg_name": arg_name, "arg_type": arg_type.__name__},
    )


def notColumnOrIntegerOrStringError(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    return PySparkException(
        error_class="NOT_COLUMN_OR_INTEGER_OR_STRING",
        message_parameters={"arg_name": arg_name, "arg_type": arg_type.__name__},
    )


def columnInListError(func_name: str) -> "PySparkException":
    return PySparkException(
        error_class="COLUMN_IN_LIST", message_parameters={"func_name": func_name}
    )


def invalidNumberOfColumnsError(func_name: str) -> "PySparkException":
    return PySparkException(
        error_class="WRONG_NUM_COLUMNS", message_parameters={"func_name": func_name}
    )


def notColumnError(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    return PySparkException(
        error_class="NOT_A_COLUMN",
        message_parameters={"arg_name": arg_name, "arg_type": arg_type.__name__},
    )


def notStringError(arg_name: str, arg_type: Type[Any]) -> "PySparkException":
    return PySparkException(
        error_class="NOT_A_STRING",
        message_parameters={"arg_name": arg_name, "arg_type": arg_type.__name__},
    )


def invalidHigherOrderFunctionArgumentNumberError(
    func_name: str, num_args: Union[str, int]
) -> "PySparkException":
    num_args = str(num_args) if isinstance(num_args, int) else num_args
    return PySparkException(
        error_class="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
        message_parameters={"func_name": func_name, "num_args": num_args},
    )


def HigherOrderFunctionShouldReturnColumnError(
    func_name: str, return_type: Type[Any]
) -> "PySparkException":
    return PySparkException(
        error_class="HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
        message_parameters={"func_name": func_name, "return_type": return_type.__name__},
    )


def invalidParameterTypeForHigherOrderFunctionError(func_name: str) -> "PySparkException":
    return PySparkException(
        error_class="UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
        message_parameters={"func_name": func_name},
    )
