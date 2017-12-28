/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.multikafka;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.multikafka.loader.KafkaConsumerLoader;
import com.streamsets.pipeline.stage.origin.multikafka.loader.MockKafkaConsumerLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMultiKafkaSource {
  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    KafkaConsumerLoader.isTest = true;
  }

  @After
  public void tearDown() {
    KafkaConsumerLoader.isTest = false;
  }

  private MultiKafkaBeanConfig getConfig() {
    MultiKafkaBeanConfig conf = new MultiKafkaBeanConfig();
    conf.consumerGroup = "sdc";
    conf.maxBatchSize = 9;
    conf.batchWaitTime = 5000;
    conf.produceSingleRecordPerMessage = false;
    conf.kafkaOptions = new HashMap<>();
    conf.brokerURI = "127.0.0.1:1234";
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.dataFormatConfig.textMaxLineLen = 4096;

    return conf;
  }

  @Test
  public void testProduceStringRecords() throws StageException, InterruptedException {
    MultiKafkaBeanConfig conf = getConfig();
    conf.topicList = Collections.singletonList("topic");
    conf.numberOfThreads = 1;

    ConsumerRecords<String, byte[]> consumerRecords = generateConsumerRecords(5, "topic", 0);
    ConsumerRecords<String, byte[]> emptyRecords = generateConsumerRecords(0, "topic", 0);

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);

    MockKafkaConsumerLoader.consumers = consumerList.iterator();
    MultiKafkaSource source = new MultiKafkaSource(conf);
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MultiKafkaDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, 1);
    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(5, records);
      Assert.assertFalse(source.isRunning());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test
  public void testMultiplePartitions() throws StageException, InterruptedException {
    MultiKafkaBeanConfig conf = getConfig();
    conf.topicList = Collections.singletonList("topic");
    conf.numberOfThreads = 1;

    ConsumerRecords<String, byte[]> consumerRecords1 = generateConsumerRecords(5, "topic", 0);
    ConsumerRecords<String, byte[]> consumerRecords2 = generateConsumerRecords(5, "topic", 1);
    ConsumerRecords<String, byte[]> emptyRecords = generateConsumerRecords(0, "topic", 0);

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito
        .when(mockConsumer.poll(conf.batchWaitTime))
        .thenReturn(consumerRecords1)
        .thenReturn(consumerRecords2)
        .thenReturn(emptyRecords);

    MockKafkaConsumerLoader.consumers = consumerList.iterator();
    MultiKafkaSource source = new MultiKafkaSource(conf);
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MultiKafkaDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, 2);
    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(10, records);
      Assert.assertFalse(source.isRunning());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test
  public void testMultipleTopics() throws StageException, InterruptedException, ExecutionException {
    MultiKafkaBeanConfig conf = getConfig();
    conf.numberOfThreads = 100;
    int numTopics = 20;
    long totalMessages = 0;
    Random rand = new Random();

    List<String> topicNames = new ArrayList<>(numTopics);
    List<KafkaConsumer> consumerList = new ArrayList<>(numTopics);

    for(int i=0; i<numTopics; i++) {
      String topic = "topic-" + i;
      topicNames.add(topic);
    }

    for(int i=0; i<conf.numberOfThreads; i++) {
      int numMessages = rand.nextInt(40)+1;
      totalMessages += numMessages;
      ConsumerRecords<String, byte[]> consumerRecords = generateConsumerRecords(numMessages, topicNames.get(rand.nextInt(numTopics)), 0);
      ConsumerRecords<String, byte[]> emptyRecords = generateConsumerRecords(0, topicNames.get(rand.nextInt(numTopics)), 0);

      KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
      consumerList.add(mockConsumer);

      Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);
    }

    conf.topicList = topicNames;

    MockKafkaConsumerLoader.consumers = consumerList.iterator();
    MultiKafkaSource source = new MultiKafkaSource(conf);
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MultiKafkaDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);

    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);
      int records = callback.waitForAllBatches();

      source.await();
      Assert.assertEquals(totalMessages, records);
      Assert.assertFalse(source.isRunning());
    } finally {
      sourceRunner.runDestroy();
    }
  }

  @Test(expected = ExecutionException.class)
  public void testPollFail() throws StageException, InterruptedException, ExecutionException {
    MultiKafkaBeanConfig conf = getConfig();
    conf.topicList = Collections.singletonList("topic");
    conf.numberOfThreads = 1;

    KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
    List<KafkaConsumer> consumerList = Collections.singletonList(mockConsumer);
    Mockito
        .when(mockConsumer.poll(conf.batchWaitTime))
        .thenThrow(new IllegalStateException());

    MockKafkaConsumerLoader.consumers = consumerList.iterator();
    MultiKafkaSource source = new MultiKafkaSource(conf);
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MultiKafkaDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);
    sourceRunner.runProduce(new HashMap<>(), 5, callback);

    //IllegalStateException in source's threads cause a StageException
    //StageException is caught by source's executor service and packaged into ExecutionException
    //ExecutionException is unpacked by source and thrown as StageException
    //sourceRunner sees this and throws as RuntimeException
    //sourceRunner's executor service then packages as ExecutionException
    try {
      sourceRunner.waitOnProduce();
    } catch (ExecutionException e) {
      Throwable except = e.getCause().getCause();
      Assert.assertEquals(StageException.class, except.getClass());
      Assert.assertEquals(KafkaErrors.KAFKA_29, ((StageException) except).getErrorCode());
      throw e;
    } finally {
      sourceRunner.runDestroy();
    }
    Assert.fail();
  }

  // If the main thread gets interrupted, then the origin (rightfully so) won't wait on all the
  // other threads that might be running. Which will subsequently intefere with other tests.
//  @Test(expected = InterruptedException.class)
  public void testInterrupt() throws StageException, InterruptedException, ExecutionException {
    MultiKafkaBeanConfig conf = getConfig();
    conf.numberOfThreads = 10;

    int numTopics = conf.numberOfThreads;
    List<String> topicNames = new ArrayList<>(numTopics);
    List<KafkaConsumer> consumerList = new ArrayList<>(numTopics);

    for(int i=0; i<numTopics; i++) {
      String topic =  "topic-" + i;
      topicNames.add(topic);
      ConsumerRecords<String, byte[]> consumerRecords = generateConsumerRecords(5, topic, 0);
      ConsumerRecords<String, byte[]> emptyRecords = generateConsumerRecords(0, topic, 0);

      KafkaConsumer mockConsumer = Mockito.mock(KafkaConsumer.class);
      consumerList.add(mockConsumer);

      Mockito.when(mockConsumer.poll(conf.batchWaitTime)).thenReturn(consumerRecords).thenReturn(emptyRecords);
    }

    conf.topicList = topicNames;

    MockKafkaConsumerLoader.consumers = consumerList.iterator();
    MultiKafkaSource source = new MultiKafkaSource(conf);
    PushSourceRunner sourceRunner = new PushSourceRunner.Builder(MultiKafkaDSource.class, source)
        .addOutputLane("lane")
        .build();
    sourceRunner.runInit();

    MultiKafkaPushSourceTestCallback callback = new MultiKafkaPushSourceTestCallback(sourceRunner, conf.numberOfThreads);

    try {
      sourceRunner.runProduce(new HashMap<>(), 5, callback);

      //start the interrupt cascade
      Thread.currentThread().interrupt();

      sourceRunner.waitOnProduce();
    } finally {
      sourceRunner.runDestroy();
    }
    Assert.fail();
  }

  private ConsumerRecords<String, byte[]> generateConsumerRecords(int count, String topic, int partition) {
    List<ConsumerRecord<String, byte[]>> consumerRecordsList = new ArrayList<>();
    for(int i=0; i<count; i++) {
      consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, "key" + i, ("value" + i).getBytes()));
    }

    Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topic, partition), consumerRecordsList);
    return new ConsumerRecords<>(recordsMap);
  }

  static class MultiKafkaPushSourceTestCallback implements PushSourceRunner.Callback {
    private final PushSourceRunner pushSourceRunner;
    private final AtomicInteger batchesProduced;
    private final AtomicInteger recordsProcessed;
    private final int numberOfBatches;

    MultiKafkaPushSourceTestCallback(PushSourceRunner pushSourceRunner, int numberOfBatches) {
      this.pushSourceRunner = pushSourceRunner;
      this.numberOfBatches = numberOfBatches;
      this.batchesProduced = new AtomicInteger(0);
      this.recordsProcessed = new AtomicInteger(0);
    }

    synchronized int waitForAllBatches() {
      try {
        pushSourceRunner.waitOnProduce();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return recordsProcessed.get();
    }

    @Override
    public void processBatch(StageRunner.Output output) {
      List<Record> records = output.getRecords().get("lane");
      if (!records.isEmpty()) {
        recordsProcessed.addAndGet(records.size());
        if (batchesProduced.incrementAndGet() == numberOfBatches) {
          pushSourceRunner.setStop();
        }
      }
    }
  }
}
