/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  public void put(OffsetAndResult<Map.Entry> batch) throws InterruptedException {
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
      // now wait for offset commit
      while (true) {
        for (ControlChannel.Message controlMessage : controlChannel.getProducerMessages()) {
          switch (controlMessage.getType()) {
            case CONSUMER_COMMIT:
              if (!expectedOffset.equals(controlMessage.getPayload())) {
                LOG.warn("Expected offset: '{}' and found: '{}'", expectedOffset, controlMessage.getPayload());
              } else if (LOG.isTraceEnabled()) {
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

  public boolean inErrorState() {
    return consumerError != null || producerError != null;
  }
}
