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
package com.streamsets.pipeline.kafka.common;

import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Category(SingleForkNoReuseTest.class)
public class PublishToKafkaIT {

  private static final String TOPIC = "testTopic";
  private static final int MULTIPLE_PARTITIONS = 1;

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9001");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "-1");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<>(config);

    CountDownLatch startProducing = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC, MULTIPLE_PARTITIONS, producer, startProducing, DataType.JSON,
      Mode.ARRAY_OBJECTS, -1, null, SdcKafkaTestUtilFactory.getInstance().create()));

    startProducing.countDown();

  }
}
