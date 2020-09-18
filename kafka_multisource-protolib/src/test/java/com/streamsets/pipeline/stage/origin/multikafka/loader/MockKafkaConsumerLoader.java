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
package com.streamsets.pipeline.stage.origin.multikafka.loader;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Test provider for the KafkaConsumerLoader that is expect test to set iterators of pre-created Kafka consumers
 * that will then subsequently be provided to the origin. This class will handle the wrapping of kafka's own
 * KafkaConsumer to SDC wrapper structure.
 */
public class MockKafkaConsumerLoader extends KafkaConsumerLoader {

  /**
   * Set this in the test to list of consumers that should be used in the origin.
   */
  public static Iterator<Consumer> consumers;

  @Override
  protected void validateConsumerConfiguration(
      Properties properties,
      Stage.Context context,
      KafkaAutoOffsetReset kafkaAutoOffsetReset,
      long timestampToSearchOffsets,
      List<String> topicsList
  ) throws StageException {
    // Do nothing for testing
  }

  /**
   * Creating consumer will just delegate to the iterator.
   */
  @Override
  protected MultiSdcKafkaConsumer createConsumerInternal(Properties properties) {
    return new WrapperKafkaConsumer(consumers.next());
  }

  /**
   * Wrapper around the KafkaConsumer that will simply delegate all the important methods.
   */
  private class WrapperKafkaConsumer implements MultiSdcKafkaConsumer {

    private Consumer delegate;

    public WrapperKafkaConsumer(Consumer consumer) {
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

    @Override
    public void commitSync(Map offsetsMap) { delegate.commitSync(offsetsMap); }
  }
}
