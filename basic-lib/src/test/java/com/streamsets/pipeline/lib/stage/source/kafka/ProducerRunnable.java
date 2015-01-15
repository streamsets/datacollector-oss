/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;

public class ProducerRunnable implements Runnable {

  private final ZkClient zkClient;
  private final KafkaServer kafkaServer;
  private final String topic;
  private final int partition;
  private final Producer<String, String> producer;
  private final CountDownLatch startLatch;

  public ProducerRunnable(ZkClient zkClient, KafkaServer kafkaServer, String topic, int partition,
                          Producer<String, String> producer,
                          CountDownLatch startLatch) {
    this.zkClient = zkClient;
    this.kafkaServer = kafkaServer;
    this.topic = topic;
    this.partition = partition;
    this.producer = producer;
    this.startLatch = startLatch;
  }

  @Override
  public void run() {
    try {
      startLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    int i = 0;
    while(true) {
      producer.send(new KeyedMessage<>(topic, String.valueOf(partition), "Hello Kafka" + i++));
    }
  }
}
