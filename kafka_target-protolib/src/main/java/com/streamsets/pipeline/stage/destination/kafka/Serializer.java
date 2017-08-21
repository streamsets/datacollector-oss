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
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.Label;

import static com.streamsets.pipeline.stage.destination.kafka.Serializer.SerializerConstants.BYTE_ARRAY_SERIALIZER;
import static com.streamsets.pipeline.stage.destination.kafka.Serializer.SerializerConstants.KAFKA_AVRO_SERIALIZER;
import static com.streamsets.pipeline.stage.destination.kafka.Serializer.SerializerConstants.STRING_SERIALIZER;

public enum Serializer implements Label {
  STRING("String", STRING_SERIALIZER, STRING_SERIALIZER),
  DEFAULT("Default", BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER),
  CONFLUENT("Confluent",KAFKA_AVRO_SERIALIZER, BYTE_ARRAY_SERIALIZER);

  class SerializerConstants {
    static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    static final String KAFKA_AVRO_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    private SerializerConstants() {}
  }

  private final String label;
  private final String keyClass;
  private final String valueClass;

  Serializer(String label, String keyClass, String valueClass) {
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
