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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

public class MultiKafkaRawSourcePreviewer implements RawSourcePreviewer {
  private final String MULTI_KAFKA_GROUP_NAME = "multiKafkaRawPreview";
  private final String KAFKA_STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
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
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "myTopic",
      label = "Topic",
      description = "Specify topic name, or leave blank to retrieve list of topics",
      displayPosition = 30
  )
  public String topic;

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
  public int pollTimeout;

  @Override
  public InputStream preview(int maxLength) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerHost + ":" + brokerPort);
    props.put("group.id", MULTI_KAFKA_GROUP_NAME);
    props.put("enable.auto.commit", "false");
    props.put(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
    props.put("key.deserializer", KAFKA_STRING_DESERIALIZER);
    props.put("value.deserializer", KAFKA_STRING_DESERIALIZER);
    ByteArrayOutputStream baos;

    try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      baos = new ByteArrayOutputStream(maxLength);
      if(topic.isEmpty()) {
        baos.write(convertToRawListString(consumer.listTopics().keySet()).getBytes());
      } else {
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
        if(records.isEmpty()) {
          baos.write("<no messages>".getBytes());
        } else {
          for (ConsumerRecord<String, String> record : records) {
            baos.write(record.value().getBytes());
          }
        }
      }

      baos.flush();
      baos.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new ByteArrayInputStream(baos.toByteArray());
  }

  //theoretically they could just copy/paste the output into the Topics bulk edit field in the Kafka config
  private <T> String convertToRawListString(Collection<T> collection) {
    return "[ " + collection.stream().map(x -> '"'+x.toString()+'"').collect(Collectors.joining(", ")) + " ]";
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
