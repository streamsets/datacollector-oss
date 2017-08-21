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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.kafka.api.LowLevelConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumerFactory;

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
    LowLevelConsumerFactorySettings lowLevelConsumerFactorySettings =
      new LowLevelConsumerFactorySettings(
          topic,
          partition,
          brokerHost,
          brokerPort,
          CLIENT_PREFIX + DOT + topic + DOT + partition,
          0,
          maxLength,
          maxWaitTime
      );
    SdcKafkaLowLevelConsumer kafkaConsumer =
      SdcKafkaLowLevelConsumerFactory.create(
          lowLevelConsumerFactorySettings
      ).create();
    try {
      kafkaConsumer.init();
      List<MessageAndOffset> messages = kafkaConsumer.read(kafkaConsumer.getOffsetToRead(true));
      ByteArrayOutputStream bOut = new ByteArrayOutputStream(maxLength);
      for(MessageAndOffset m : messages) {
        bOut.write((byte[])m.getPayload());
      }
      bOut.flush();
      bOut.close();
      return new ByteArrayInputStream(bOut.toByteArray());
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
