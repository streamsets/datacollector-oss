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
package com.streamsets.pipeline.kafka.impl;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestKafka09ProducerFactory {

  @Test
  public void testKafka09ProducerFactory() {

    ProducerFactorySettings settings = new ProducerFactorySettings(
        new HashMap<String, Object>(),
        PartitionStrategy.DEFAULT,
        "",
        DataFormat.JSON,
        false
    );
    SdcKafkaProducerFactory sdcKafkaProducerFactory = SdcKafkaProducerFactory.create(settings);

    Assert.assertNotNull(sdcKafkaProducerFactory);
    Assert.assertTrue(sdcKafkaProducerFactory instanceof Kafka09ProducerFactory);

    SdcKafkaProducer sdcKafkaProducer = sdcKafkaProducerFactory.create();
    Assert.assertNotNull(sdcKafkaProducer);
    Assert.assertTrue(sdcKafkaProducer instanceof KafkaProducer09);
  }
}
