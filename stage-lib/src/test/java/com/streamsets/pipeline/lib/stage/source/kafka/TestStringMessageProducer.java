/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Class used to generate string messages.
 * This is used to experiment with the kafka source.
 */
public class TestStringMessageProducer {

  public static void main(String[] args) throws InterruptedException {
    Random rnd = new Random();

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.streamsets.pipeline.lib.stage.source.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<>(config);
    int counter = 0;
    while(true) {
      long runtime = new Date().getTime();
      String ip = "192.168.2.0" + rnd.nextInt(255);
      String msg = runtime + ",www.example.com," + ip;
      KeyedMessage<String, String> data = new KeyedMessage<>("ssTopic3", ip, msg);
      producer.send(data);
      System.out.println("Record counter: " + counter++);
    }
  }
}
