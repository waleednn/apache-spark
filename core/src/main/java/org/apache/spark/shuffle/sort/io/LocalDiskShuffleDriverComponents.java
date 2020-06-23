/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort.io;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.metadata.ShuffleOutputTracker;

public class LocalDiskShuffleDriverComponents implements ShuffleDriverComponents {

  private final LocalDiskShuffleOutputTracker outputTracker;

  public LocalDiskShuffleDriverComponents(SparkConf sparkConf) {
    this.outputTracker = new LocalDiskShuffleOutputTracker(sparkConf);
  }

  @Override
  public Map<String, String> getAddedExecutorSparkConf() {
    return Collections.emptyMap();
  }

  @Override
  public void cleanupApplication() {
    // nothing to clean up
  }

  @Override
  public ShuffleOutputTracker shuffleOutputTracker() {
    return outputTracker;
  }
}
