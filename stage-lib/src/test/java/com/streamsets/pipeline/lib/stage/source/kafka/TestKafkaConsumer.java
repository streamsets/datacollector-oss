/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import java.util.List;

public class TestKafkaConsumer {

  public static void main(String[] args) throws Exception {
    KafkaConsumer kafkaConsumer = new KafkaConsumer("AA", 1, new KafkaBroker("localhost", 9001), 4000,
      64000, 1000);
    kafkaConsumer.init();

    List<MessageAndOffset> read = kafkaConsumer.read(0);
    for(MessageAndOffset m : read) {
      System.out.println(new String(m.getPayload()));
    }
  }
}
