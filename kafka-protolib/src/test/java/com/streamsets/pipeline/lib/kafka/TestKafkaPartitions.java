/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.SimpleConsumer;
import scala.collection.Iterator;

import java.nio.ByteBuffer;

public class TestKafkaPartitions {

  private static final int PARTITION = 1;
  private static final String TOPIC = "mytopic";

  public static void main(String[] args) {
    SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 9001, 10000, 8000, "client");
    FetchRequest fetchRequest = new FetchRequestBuilder()
      .clientId("hello")
      .minBytes(0)
      .maxWait(5000)
      .addFetch(TOPIC, PARTITION, 0, 1000)
      .build();
    FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
    if(fetchResponse.hasError()) {
      System.out.println("Error code : " + fetchResponse.errorCode(TOPIC, PARTITION));
    }
    Iterator<kafka.message.MessageAndOffset> iterator = fetchResponse.messageSet(TOPIC, PARTITION).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      ByteBuffer payload  = iterator.next().message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      System.out.println(new String(bytes));
      count++;
    }
    System.out.println("Read " + count + " messages");
  }
}
