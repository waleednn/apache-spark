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

package org.apache.spark.network.shuffle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.google.common.io.CharStreams;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class ExternalShuffleBlockResolverSuite {
  private static final String sortBlock0 = "Hello!";
  private static final String sortBlock1 = "World!";
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static TestShuffleDataContext dataContext;

  private static final TransportConf conf =
      new TransportConf("shuffle", MapConfigProvider.EMPTY);

  @BeforeClass
  public static void beforeAll() throws IOException {
    dataContext = new TestShuffleDataContext(2, 5);

    dataContext.create();
    // Write some sort data.
    dataContext.insertSortShuffleData(0, 0, new byte[][] {
        sortBlock0.getBytes(StandardCharsets.UTF_8),
        sortBlock1.getBytes(StandardCharsets.UTF_8)});
  }

  @AfterClass
  public static void afterAll() {
    dataContext.cleanup();
  }

  @Test
  public void testBadRequests() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    // Unregistered executor
    try {
      resolver.getBlockData("app0", "exec1", 1, 1, 0);
      fail("Should have failed");
    } catch (RuntimeException e) {
      assertTrue("Bad error message: " + e, e.getMessage().contains("not registered"));
    }

    // Nonexistent shuffle block
    resolver.registerExecutor("app0", "exec3",
      dataContext.createExecutorInfo(SORT_MANAGER));
    try {
      resolver.getBlockData("app0", "exec3", 1, 1, 0);
      fail("Should have failed");
    } catch (Exception e) {
      // pass
    }
  }

  @Test
  public void testSortShuffleBlocks() throws IOException {
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(conf, null);
    resolver.registerExecutor("app0", "exec0",
      dataContext.createExecutorInfo(SORT_MANAGER));

    try (InputStream block0Stream = resolver.getBlockData(
        "app0", "exec0", 0, 0, 0).createInputStream()) {
      String block0 =
        CharStreams.toString(new InputStreamReader(block0Stream, StandardCharsets.UTF_8));
      assertEquals(sortBlock0, block0);
    }

    try (InputStream block1Stream = resolver.getBlockData(
        "app0", "exec0", 0, 0, 1).createInputStream()) {
      String block1 =
        CharStreams.toString(new InputStreamReader(block1Stream, StandardCharsets.UTF_8));
      assertEquals(sortBlock1, block1);
    }

    try (InputStream blocksStream = resolver.getContinuousBlocksData(
        "app0", "exec0", 0, 0, 0, 2).createInputStream()) {
      String blocks =
        CharStreams.toString(new InputStreamReader(blocksStream, StandardCharsets.UTF_8));
      assertEquals(sortBlock0 + sortBlock1, blocks);
    }
  }

  @Test
  public void testShuffleIndexCacheEvictionBehavior() throws IOException, ExecutionException {
    Map<String, String> config = new HashMap<>();
    String indexCacheSize = "8192m";
    config.put("spark.shuffle.service.index.cache.size", indexCacheSize);
    TransportConf transportConf = new TransportConf("shuffle", new MapConfigProvider(config));
    ExternalShuffleBlockResolver resolver = new ExternalShuffleBlockResolver(transportConf, null);
    resolver.registerExecutor("app0", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));

    LoadingCache<File, ShuffleIndexInformation> shuffleIndexCache = resolver.shuffleIndexCache;

    // 8g -> 8589934592 bytes
    long maximumWeight = JavaUtils.byteStringAsBytes(indexCacheSize);
    int unitSize = 1048575;
    // CacheBuilder.DEFAULT_CONCURRENCY_LEVEL
    int concurrencyLevel = 4;
    int totalGetCount = 16384;
    // maxCacheCount is 8192
    long maxCacheCount = maximumWeight / concurrencyLevel / unitSize * concurrencyLevel;
    for (int i = 0; i < totalGetCount; i++) {
      File indexFile = new File("shuffle_" + 0 + "_" + i + "_0.index");
      ShuffleIndexInformation indexInfo = Mockito.mock(ShuffleIndexInformation.class);
      Mockito.when(indexInfo.getSize()).thenReturn(unitSize);
      shuffleIndexCache.get(indexFile, () -> indexInfo);
    }

    long totalWeight =
      shuffleIndexCache.asMap().values().stream().mapToLong(ShuffleIndexInformation::getSize).sum();
    long size = shuffleIndexCache.size();
    try{
      Assert.assertTrue(size <= maxCacheCount);
      Assert.assertTrue(totalWeight < maximumWeight);
      fail("The tests code should not enter this line now.");
    } catch (AssertionError error) {
      // The code will enter this branch because LocalCache weight eviction does not work
      // when maxSegmentWeight is >= Int.MAX_VALUE.
      // TODO remove cache AssertionError after fix this bug.
      Assert.assertTrue(size > maxCacheCount && size <= totalGetCount);
      Assert.assertTrue(totalWeight > maximumWeight);
    }
  }

  @Test
  public void jsonSerializationOfExecutorRegistration() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    AppExecId appId = new AppExecId("foo", "bar");
    String appIdJson = mapper.writeValueAsString(appId);
    AppExecId parsedAppId = mapper.readValue(appIdJson, AppExecId.class);
    assertEquals(parsedAppId, appId);

    ExecutorShuffleInfo shuffleInfo =
      new ExecutorShuffleInfo(new String[]{"/bippy", "/flippy"}, 7, SORT_MANAGER);
    String shuffleJson = mapper.writeValueAsString(shuffleInfo);
    ExecutorShuffleInfo parsedShuffleInfo =
      mapper.readValue(shuffleJson, ExecutorShuffleInfo.class);
    assertEquals(parsedShuffleInfo, shuffleInfo);

    // Intentionally keep these hard-coded strings in here, to check backwards-compatibility.
    // its not legacy yet, but keeping this here in case anybody changes it
    String legacyAppIdJson = "{\"appId\":\"foo\", \"execId\":\"bar\"}";
    assertEquals(appId, mapper.readValue(legacyAppIdJson, AppExecId.class));
    String legacyShuffleJson = "{\"localDirs\": [\"/bippy\", \"/flippy\"], " +
      "\"subDirsPerLocalDir\": 7, \"shuffleManager\": " + "\"" + SORT_MANAGER + "\"}";
    assertEquals(shuffleInfo, mapper.readValue(legacyShuffleJson, ExecutorShuffleInfo.class));
  }

}
