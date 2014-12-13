/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import java.nio.ByteBuffer;
import java.util.List;

public class TestKafkaConsumer {

  public static void main(String[] args) throws Exception {
    KafkaConsumer kafkaConsumer = new KafkaConsumer("GG1", 0, new KafkaBroker("localhost", 9001), 4000,
      640000, 1000);
    kafkaConsumer.init();

    List<MessageAndOffset> read = kafkaConsumer.read(0);
    for(MessageAndOffset m : read) {
      ByteBuffer payload  = m.getPayload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      System.out.println(new String(bytes));
    }
  }
}
