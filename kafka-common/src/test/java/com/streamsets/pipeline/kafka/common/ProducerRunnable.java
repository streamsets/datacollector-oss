/*
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
package com.streamsets.pipeline.kafka.common;

import com.streamsets.pipeline.api.ext.json.Mode;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ProducerRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProducerRunnable.class);

  private final String topic;
  private final Producer<String, String> producer;
  private final CountDownLatch startLatch;
  private final DataType dataType;
  private final int partitions;
  private int lastPartition;
  private final Mode jsonMode;
  private int noOfRecords;
  private final CountDownLatch doneSignal;
  private final SdcKafkaTestUtil testUtil;

  public ProducerRunnable(
    String topic,
    int partitions,
    Producer<String, String> producer,
    CountDownLatch startLatch,
    DataType dataType,
    Mode jsonMode,
    int noOfRecords,
    CountDownLatch doneSignal,
    SdcKafkaTestUtil testUtil
  ) {
    this.topic = topic;
    this.partitions = partitions;
    this.producer = producer;
    this.startLatch = startLatch;
    this.dataType = dataType;
    this.lastPartition = 0;
    this.jsonMode = jsonMode;
    this.noOfRecords = noOfRecords;
    this.doneSignal = doneSignal;
    this.testUtil = testUtil;

    LOG.info(
      "Received producer class {} from {}",
      producer.getClass(),
      producer.getClass().getProtectionDomain().getCodeSource().getLocation().toString()
    );
    LOG.info(
      "Received test util {} from {}",
      testUtil.getClass(),
      testUtil.getClass().getProtectionDomain().getCodeSource().getLocation().toString()
    );
  }

  @Override
  public void run() {
    try {
      LOG.info("Waiting on signal to start");
      startLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Ignoring exception", e);
    }

    LOG.info("Moving to generate messages");

    try {
      int i = 0;
      while (i < noOfRecords || noOfRecords == -1) {
        producer.send(new KeyedMessage<>(topic, getPartitionKey(), testUtil.generateTestData(dataType, jsonMode)));
        i++;
      }
      if (doneSignal != null) {
        doneSignal.countDown();
      }
    } catch (Throwable e) {
      LOG.error("Exception while generating messages: {}", e.toString(), e);
      throw new RuntimeException("Can't generate messages", e);
    }
  }

  private String getPartitionKey() {
    lastPartition = (lastPartition + 1) % partitions;
    return String.valueOf(lastPartition);
  }
}
