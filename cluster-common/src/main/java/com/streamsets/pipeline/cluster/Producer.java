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

import com.google.common.base.Throwables;
import com.streamsets.pipeline.impl.OffsetAndResult;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implements the producer side of the cluster producer consumer pattern.
 */
public class Producer {
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
  private final ControlChannel controlChannel;
  private final DataChannel dataChannel;
  private volatile Throwable consumerError;
  private volatile Throwable producerError;

  public Producer(ControlChannel controlChannel, DataChannel dataChannel) {
    this.controlChannel = controlChannel;
    this.dataChannel = dataChannel;
  }

  /**
   * Send a control message indicating the producer has completed.
   */
  public void complete() {
    controlChannel.producerComplete();
  }

  /**
   * Queues the batch for the consumer and waits until the consumer
   * successfully commits the batch. While waiting processes any
   * control messages from the consumer. Throws an exception
   * when the consumer has indicated it encountered an error.
   */
  public Object put(OffsetAndResult<Map.Entry> batch) {
    if (consumerError != null) {
      throw new RuntimeException(Utils.format("Consumer encountered error: {}", consumerError), consumerError);
    }
    if (producerError != null) {
      throw new RuntimeException(Utils.format("Producer encountered error: {}", producerError), producerError);
    }
    try {
      Object expectedOffset = "EMPTY_BATCH";
      if (!batch.getResult().isEmpty()) {
        expectedOffset = batch.getResult().get(batch.getResult().size() - 1).getKey(); // get the last one
      }
      while (!dataChannel.offer(batch, 10, TimeUnit.MILLISECONDS)) {
        for (ControlChannel.Message controlMessage : controlChannel.getProducerMessages()) {
          switch (controlMessage.getType()) {
            case CONSUMER_ERROR:
              Throwable throwable = (Throwable) controlMessage.getPayload();
              consumerError = throwable;
              throw new ConsumerRuntimeException(Utils.format("Consumer encountered error: {}", throwable), throwable);
            default:
              String msg = Utils.format("Illegal control message type: '{}'", controlMessage.getType());
              throw new IllegalStateException(msg);
          }
        }
      }
      return expectedOffset;
    } catch (Throwable throwable) {
      controlChannel.producerComplete();
      if (!(throwable instanceof ConsumerRuntimeException)) {
        String msg = "Error caught in producer: " + throwable;
        LOG.error(msg, throwable);
        controlChannel.producerError(throwable);
        if (producerError == null) {
          producerError = throwable;
        }
      }
      throw Throwables.propagate(throwable);
    }
  }

  public void waitForCommit() throws InterruptedException {
    while (true) {
      for (ControlChannel.Message controlMessage : controlChannel.getProducerMessages()) {
        switch (controlMessage.getType()) {
          case CONSUMER_COMMIT:
            if (LOG.isTraceEnabled()) {
              LOG.trace("Commit of: '{}'", controlMessage.getPayload());
            }
            return;
          case CONSUMER_ERROR:
            Throwable throwable = (Throwable) controlMessage.getPayload();
            consumerError = throwable;
            throw new ConsumerRuntimeException(Utils.format("Consumer encountered error: {}", throwable), throwable);
          default:
            throw new IllegalStateException(Utils.format("Illegal control message type: '{}'",
              controlMessage.getType()));
        }
      }
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }

  public boolean inErrorState() {
    return consumerError != null || producerError != null;
  }
}
