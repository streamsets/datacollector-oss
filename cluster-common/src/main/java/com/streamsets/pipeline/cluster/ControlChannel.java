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
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Provides a bi-directional control channel between two threads, one producing
 * and the other consuming. Both threads can queue multiple control messages
 * for the corresponding thread.
 */
public class ControlChannel {
  private static final Logger LOG = LoggerFactory.getLogger(ControlChannel.class);
  private final BlockingQueue<Message> producerQueue = new ArrayBlockingQueue<>(10);
  private final BlockingQueue<Message> consumerQueue = new ArrayBlockingQueue<>(10);

  public void producerComplete() {
    LOG.info("Producer complete");
    try {
      consumerQueue.put(new Message(MessageType.PRODUCER_COMPLETE));
    } catch (InterruptedException e) {
      LOG.info("Interrupted while queuing '{}'", MessageType.PRODUCER_COMPLETE.name());
      Thread.currentThread().interrupt();
    }
  }

  public List<Message> getProducerMessages() {
    List<Message> result = new ArrayList<>();
    producerQueue.drainTo(result);
    return result;
  }

  public List<Message> getConsumerMessages() {
    List<Message> result = new ArrayList<>();
    consumerQueue.drainTo(result);
    return result;
  }

  public void consumerError(Throwable throwable) {
    LOG.trace("Consumer Error: {}", throwable, throwable);
    try {
      producerQueue.put(new Message(MessageType.CONSUMER_ERROR, throwable));
    } catch (InterruptedException e) {
      LOG.info("Interrupted while queuing '{}': {}", MessageType.CONSUMER_ERROR.name(), throwable, throwable);
      Thread.currentThread().interrupt();
    }
  }

  public void producerError(Throwable throwable) {
    LOG.trace("Producer Error: {}", throwable, throwable);
    try {
      consumerQueue.put(new Message(MessageType.PRODUCER_ERROR, throwable));
    } catch (InterruptedException e) {
      LOG.info("Interrupted while queuing '{}': {}", MessageType.PRODUCER_ERROR.name(), throwable, throwable);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * If a null value is passed to this method, it's replaced with
   * a dummy due to the fact the payload for each message is wrapped
   * in an Optional.
   */
  public void consumerCommit(String offset) {
    Object offsetValue = offset;
    if (offsetValue == null) {
      offsetValue = new NullOffset();
    }
    LOG.trace("Commit Offset: '{}'", offsetValue);
    try {
      producerQueue.put(new Message(MessageType.CONSUMER_COMMIT, offsetValue));
    } catch (InterruptedException e) {
      LOG.info("Interrupted while queuing '{}'", MessageType.CONSUMER_COMMIT.name(), offsetValue);
      Thread.currentThread().interrupt();
    }
  }

  private static class NullOffset {
    @Override
    public String toString() {
      return "(null offset)";
    }
  }

  public static class Message {
    private final MessageType type;
    private final Object payload;

    public Message(MessageType type) {
      this(type, null);
    }
    public Message(MessageType type, Object payload) {
      this.type = type;
      this.payload = payload;
      if (type.hasPayload && this.payload == null) {
        throw new IllegalStateException(Utils.format("Message type '{}' requires payload", type.name()));
      }
    }

    public MessageType getType() {
      return type;
    }

    public Object getPayload() {
      return payload;
    }
  }

  public enum MessageType {
    PRODUCER_COMPLETE(false), // inform consumer to return null offset, thus shutting down the pipeline
    CONSUMER_ERROR(true), // inform producer consumer is dead due to an error condition
    PRODUCER_ERROR(true), // inform consumer producer is dead due to an error condition
    CONSUMER_COMMIT(true); // inform producer offset has been committed

    private boolean hasPayload;
    MessageType(boolean hasPayload) {
      this.hasPayload = hasPayload;
    }
  }

}
