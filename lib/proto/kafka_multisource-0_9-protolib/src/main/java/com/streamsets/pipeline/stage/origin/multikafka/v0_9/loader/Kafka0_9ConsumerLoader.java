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
package com.streamsets.pipeline.stage.origin.multikafka.v0_9.loader;

import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;
import com.streamsets.pipeline.stage.origin.multikafka.loader.KafkaConsumerLoader;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class Kafka0_9ConsumerLoader extends KafkaConsumerLoader {

  @Override
  protected MultiSdcKafkaConsumer createConsumerInternal(Properties properties) {
    return new WrapperKafkaConsumer(new KafkaConsumer(properties));
  }

  /**
   * Wrapper around the KafkaConsumer that will simply delegate all the important methods.
   */
  private class WrapperKafkaConsumer implements MultiSdcKafkaConsumer {

    private KafkaConsumer delegate;

    public WrapperKafkaConsumer(KafkaConsumer consumer) {
      this.delegate = consumer;
    }

    @Override
    public void subscribe(List topics) {
      delegate.subscribe(topics);
    }

    @Override
    public ConsumerRecords poll(long timeout) {
      return delegate.poll(timeout);
    }

    @Override
    public void unsubscribe() {
      delegate.unsubscribe();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
