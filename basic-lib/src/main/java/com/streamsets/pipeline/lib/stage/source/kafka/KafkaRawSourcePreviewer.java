/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

public class KafkaRawSourcePreviewer implements RawSourcePreviewer {
  private static final String CLIENT_PREFIX = "StreamSetsKafkaPreviewer";
  private static final String DOT = ".";

  private String mimeType;

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    label = "Topic",
    defaultValue = "myTopic")
  public String topic;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Partition",
    defaultValue = "0")
  public int partition;

  @ConfigDef(required = true,
    type = ConfigDef.Type.STRING,
    description = "A known kafka broker. Does not have to be the leader of the partition",
    label = "Broker Host",
    defaultValue = "localhost")
  public String brokerHost;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    label = "Broker Port",
    defaultValue = "9092")
  public int brokerPort;

  @ConfigDef(required = true,
    type = ConfigDef.Type.INTEGER,
    description = "The maximum wait time in seconds before the kafka fetch request returns if no message is available.",
    label = "Max Wait Time",
    defaultValue = "1000")
  public int maxWaitTime;

  @Override
  public InputStream preview(int maxLength) {
    KafkaConsumer kafkaConsumer = new KafkaConsumer(topic, partition, new KafkaBroker(brokerHost, brokerPort),
      0, maxLength, maxWaitTime, CLIENT_PREFIX + DOT + topic + DOT + partition);
    try {
      kafkaConsumer.init();
      List<MessageAndOffset> messages = kafkaConsumer.read(kafkaConsumer.getOffsetToRead(true));
      ByteArrayOutputStream bOut = new ByteArrayOutputStream(maxLength);
      for(MessageAndOffset m : messages) {
        bOut.write(m.getPayload());
      }
      bOut.flush();
      bOut.close();
      ByteArrayInputStream bIn = new ByteArrayInputStream(bOut.toByteArray());
      return bIn;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public String getMimeType() {
    return mimeType;
  }

  @Override
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }
}
