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

from pyspark.mlv2.base import Evaluator

from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol
from pyspark.mlv2.util import aggregate_dataframe
from pyspark.sql.functions import col

import torch
import torcheval.metrics as torchmetrics


class RegressionEvaluator(Evaluator, HasLabelCol, HasPredictionCol):

    metricName: Param[str] = Param(
        Params._dummy(),
        "metricName",
        "metric name for the regression evaluator.",
        typeConverter=TypeConverters.toString,
    )

    def _get_torch_metric(self):
        metric_name = self.getOrDefault(self.metricName)

        if metric_name == "mse":
            return torchmetrics.MeanSquaredError()
        if metric_name == "r2":
            return torchmetrics.R2Score()

        raise ValueError(f"Unsupported regressor evaluator metric name: {metric_name}")

    def _evaluate(self, dataset):

        prediction_col = self.getPredictionCol()
        label_col = self.getLabelCol()

        torch_metric = self._get_torch_metric()

        def local_agg_fn(pandas_df):
            preds_tensor = torch.tensor(pandas_df[prediction_col].values)
            labels_tensor = torch.tensor(pandas_df[label_col].values)
            torch_metric.update(preds_tensor, labels_tensor)
            return torch_metric

        def merge_agg_status(state1, state2):
            state1.merge_state([state2])
            return state1

        def agg_status_to_result(state):
            return state.compute().item()

        return aggregate_dataframe(
            dataset,
            [prediction_col, label_col],
            local_agg_fn,
            merge_agg_status,
            agg_status_to_result,
        )
