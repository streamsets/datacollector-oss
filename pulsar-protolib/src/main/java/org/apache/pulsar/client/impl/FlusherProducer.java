/*
 * Copyright 2018 StreamSets Inc.
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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.CompletableFuture;

// TODO, delete this wrapper SDC-9554
/*
 This Wrapper is only to expose the flush() protected method until we upgrade to Pulsar 2.2+ which exposes flush
 in the Producer interface
 */
public class FlusherProducer<T> implements Producer<T> {
  private final ProducerBase<T> producer;

  public FlusherProducer(Producer<T> producer) {
    this.producer = (ProducerBase) producer;
  }

  @Override
  public String getTopic() {
    return producer.getTopic();
  }

  @Override
  public String getProducerName() {
    return producer.getProducerName();
  }

  @Override
  public MessageId send(T t) throws PulsarClientException {
    return producer.send(t);
  }

  @Override
  public CompletableFuture<MessageId> sendAsync(T t) {
    return producer.sendAsync(t);
  }

  @Override
  public TypedMessageBuilder<T> newMessage() {
    return producer.newMessage();
  }

  @Override
  @Deprecated
  public MessageId send(Message<T> message) throws PulsarClientException {
    return producer.send(message);
  }

  @Override
  @Deprecated
  public CompletableFuture<MessageId> sendAsync(Message<T> message) {
    return producer.sendAsync(message);
  }

  @Override
  public long getLastSequenceId() {
    return producer.getLastSequenceId();
  }

  @Override
  public ProducerStats getStats() {
    return producer.getStats();
  }

  @Override
  public void close() throws PulsarClientException {
    producer.close();
  }

  @Override
  public CompletableFuture<Void> closeAsync() {
    return producer.closeAsync();
  }

  public void flush() {
    producer.flush();
  }
}
