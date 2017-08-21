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

import com.streamsets.pipeline.api.Label;

import static com.streamsets.pipeline.stage.origin.kafka.Deserializer.SerializerConstants.BYTE_ARRAY_DESERIALIZER;
import static com.streamsets.pipeline.stage.origin.kafka.Deserializer.SerializerConstants.KAFKA_AVRO_DESERIALIZER;
import static com.streamsets.pipeline.stage.origin.kafka.Deserializer.SerializerConstants.STRING_DESERIALIZER;

public enum Deserializer implements Label {
  STRING("String", STRING_DESERIALIZER, STRING_DESERIALIZER),
  DEFAULT("Default", BYTE_ARRAY_DESERIALIZER, BYTE_ARRAY_DESERIALIZER),
  CONFLUENT("Confluent", KAFKA_AVRO_DESERIALIZER, BYTE_ARRAY_DESERIALIZER);

  class SerializerConstants {
    static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    static final String KAFKA_AVRO_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private SerializerConstants() {}
  }

  private final String label;
  private final String keyClass;
  private final String valueClass;

  Deserializer(String label, String keyClass, String valueClass) {
    this.label = label;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getKeyClass() {
    return keyClass;
  }

  public String getValueClass() {
    return valueClass;
  }
}
