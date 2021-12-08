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
from datetime import timedelta

import pandas as pd
from pandas.api.types import CategoricalDtype

import pyspark.pandas as ps
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class TimedeltaOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([timedelta(1), timedelta(microseconds=2)])

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def td_pdf(self):
        psers = {
            "this": self.pser,
            "that": pd.Series([timedelta(weeks=1), timedelta(days=2)]),
        }
        return pd.concat(psers, axis=1)

    @property
    def td_psdf(self):
        return ps.from_pandas(self.td_pdf)

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser ** 1)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 2 * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1 ** self.psser)

    def test_from_to_pandas(self):
        data = [timedelta(1), timedelta(microseconds=2)]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser.to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        pser = self.pser
        psser = self.psser
        target_psser = ps.Series(
            ["INTERVAL '1 00:00:00' DAY TO SECOND", "INTERVAL '0 00:00:00.000002' DAY TO SECOND"]
        )
        self.assert_eq(target_psser, psser.astype(str))
        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=["a", "b", "c"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

        self.assertRaises(TypeError, lambda: psser.astype(bool))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        pdf, psdf = self.td_pdf, self.td_psdf
        self.assert_eq(pdf["this"] == pdf["this"], psdf["this"] == psdf["this"])
        self.assert_eq(pdf["this"] == pdf["that"], psdf["this"] == psdf["that"])

    def test_ne(self):
        pdf, psdf = self.td_pdf, self.td_psdf
        self.assert_eq(pdf["this"] != pdf["this"], psdf["this"] != psdf["this"])
        self.assert_eq(pdf["this"] != pdf["that"], psdf["this"] != psdf["that"])

    def test_lt(self):
        self.assertRaisesRegex(
            TypeError, "< can not be applied to", lambda: self.psser < self.psser
        )

    def test_le(self):
        self.assertRaisesRegex(
            TypeError, "<= can not be applied to", lambda: self.psser <= self.psser
        )

    def test_gt(self):
        self.assertRaisesRegex(
            TypeError, "> can not be applied to", lambda: self.psser > self.psser
        )

    def test_ge(self):
        self.assertRaisesRegex(
            TypeError, ">= can not be applied to", lambda: self.psser >= self.psser
        )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_timedelta_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
