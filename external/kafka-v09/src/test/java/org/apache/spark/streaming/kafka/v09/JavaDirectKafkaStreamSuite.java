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

package org.apache.spark.streaming.kafka.v09;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaDirectKafkaStreamSuite implements Serializable {
  private transient JavaStreamingContext ssc = null;
  private transient KafkaTestUtils kafkaTestUtils = null;

  @Before
  public void setUp() {
    kafkaTestUtils = new KafkaTestUtils();
    kafkaTestUtils.setup();
    SparkConf sparkConf = new SparkConf()
      .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
    ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(200));
  }

  @After
  public void tearDown() {
    if (ssc != null) {
      ssc.stop();
      ssc = null;
    }

    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown();
      kafkaTestUtils = null;
    }
  }

  @Test
  public void testKafkaStream() throws InterruptedException {
    final String topic1 = "topic1_testKafkaDirectStream";
    final String topic2 = "topic2_testKafkaDirectStream";
    // hold a reference to the current offset ranges, so it can be used downstream
    final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

    String[] topic1data = createTopicAndSendData(topic1);
    String[] topic2data = createTopicAndSendData(topic2);

    Set<String> sent = new HashSet<>();
    sent.addAll(Arrays.asList(topic1data));
    sent.addAll(Arrays.asList(topic2data));

    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestUtils.brokerAddress());
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("spark.kafka.poll.time", "1000");

    JavaDStream<String> stream1 = KafkaUtils.createDirectStream(
        ssc,
        String.class,
        String.class,
        kafkaParams,
        topicToSet(topic1)
    ).transformToPair(
        // Make sure you can get offset ranges from the rdd
        new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
          @Override
          public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);
            Assert.assertEquals(topic1, offsets[0].topic());
            return rdd;
          }
        }
    ).map(
        new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(Tuple2<String, String> kv) {
            return kv._2();
          }
        }
    );

    JavaDStream<String> stream2 = KafkaUtils.createDirectStream(
        ssc,
        String.class,
        String.class,
        String.class,
        kafkaParams,
        topicOffsetToMap(topic2, 0L),
        new Function<ConsumerRecord<String, String>, String>() {
          @Override
          public String call(ConsumerRecord<String, String> consumerRecord) throws Exception {
            return consumerRecord.value();
          }
        }
    );
    JavaDStream<String> unifiedStream = stream1.union(stream2);

    final Set<String> result = Collections.synchronizedSet(new HashSet<String>());
    unifiedStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
          @Override
          public void call(JavaRDD<String> rdd) {
            result.addAll(rdd.collect());
            for (OffsetRange o : offsetRanges.get()) {
              System.out.println(
                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
              );
            }
          }
        }
    );
    ssc.start();
    long startTime = System.currentTimeMillis();
    boolean matches = false;
    while (!matches && System.currentTimeMillis() - startTime < 20000) {
      matches = sent.size() == result.size();
      Thread.sleep(50);
    }
    Assert.assertEquals(sent, result);
    ssc.stop();
  }

  private static Set<String> topicToSet(String topic) {
    Set<String> topicSet = new HashSet<>();
    topicSet.add(topic);
    return topicSet;
  }

  private static Map<TopicPartition, Long> topicOffsetToMap(String topic, Long offsetToStart) {
    Map<TopicPartition, Long> topicMap = new HashMap<>();
    topicMap.put(new TopicPartition(topic, 0), offsetToStart);
    return topicMap;
  }

  private String[] createTopicAndSendData(String topic) {
    String[] data = {topic + "-1", topic + "-2", topic + "-3"};
    kafkaTestUtils.createTopic(topic);
    kafkaTestUtils.sendMessages(topic, data);
    return data;
  }
}
