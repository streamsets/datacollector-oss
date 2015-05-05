/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.lib.KafkaBroker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

public class KafkaRawSourcePreviewer implements RawSourcePreviewer {
  private static final String CLIENT_PREFIX = "StreamSetsKafkaPreviewer";
  private static final String DOT = ".";

  private String mimeType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Broker Host",
      description = "",
      displayPosition = 10
  )
  public String brokerHost;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "9092",
      label = "Broker Port",
      description = "",
      displayPosition = 20,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int brokerPort;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "myTopic",
      label = "Topic",
      description = "",
      displayPosition = 30
  )
  public String topic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Partition",
      description = "",
      displayPosition = 40,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int partition;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Wait Time (millisecs)",
      description = "Max time to wait for data from Kafka",
      displayPosition = 50,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime;

  @Override
  public InputStream preview(int maxLength) {
    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer(topic, partition, new KafkaBroker(brokerHost, brokerPort),
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
