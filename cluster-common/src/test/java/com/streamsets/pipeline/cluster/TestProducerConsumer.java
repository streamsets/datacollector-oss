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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestProducerConsumer {

  private ExecutorService executorService;
  private ControlChannel controlChannel;
  private DataChannel dataChannel;
  private Producer producer;
  private Consumer consumer;

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
    controlChannel = new ControlChannel();
    dataChannel = new DataChannel();
    producer = new Producer(controlChannel, dataChannel);
    consumer = new Consumer(controlChannel, dataChannel);
  }

  @After
  public void teardown() {
    executorService.shutdownNow();
  }

  @Test(timeout = 5000)
  public void testProducerComplete() throws Exception {
    Future<?> putFuture = put(1);
    producer.complete();
    Future<List<Map.Entry>> takeFuture = take(true);
    putFuture.get();
    Assert.assertEquals(createBatch(1), takeFuture.get());
    Assert.assertNull(consumer.take()); // should not block and should return null
  }

  @Test(timeout = 5000, expected = TimeoutException.class)
  public void testNoCommitTimesOutProducer() throws Exception {
    Future<?> putFuture = put(1);
    take(false);
    putFuture.get(1, TimeUnit.SECONDS);
  }

  @Test(timeout = 5000, expected = IllegalStateException.class)
  public void testNoCommitCausesIllegalStateException() throws Exception {
    put(1);
    take(false).get();
    Throwables.propagate(getError(take(false)));
  }

  @Test(timeout = 5000)
  public void testConsumerErrorCausesPutToReturn() throws Exception {
    RuntimeException consumerError = new RuntimeException();
    consumer.error(consumerError);
    Assert.assertSame(consumerError, getError(put(1)));
  }
  @Test(timeout = 5000)
  public void testConsumerErrorPassedToProducer1() throws Exception {
    RuntimeException consumerError = new RuntimeException();
    consumer.error(consumerError);
    List<ControlChannel.Message> producerMessages = controlChannel.getProducerMessages();
    Assert.assertEquals(1, producerMessages.size());
    ControlChannel.Message producerMessage = producerMessages.get(0);
    Assert.assertEquals(ControlChannel.MessageType.CONSUMER_ERROR, producerMessage.getType());
    Assert.assertSame(consumerError, producerMessage.getPayload());
  }

  @Test(timeout = 5000)
  public void testConsumerErrorPassedToProducer2() throws Exception {
    RuntimeException consumerError = new RuntimeException();
    consumer.error(consumerError);
    Assert.assertSame(consumerError, getError(put(1)));
  }

  @Test(timeout = 5000)
  public void testProducerErrorPassedToConsumer() throws Exception {
    RuntimeException producerError = new RuntimeException();
    controlChannel.producerError(producerError);
    List<ControlChannel.Message> consumerMessages = controlChannel.getConsumerMessages();
    Assert.assertEquals(1, consumerMessages.size());
    ControlChannel.Message consumerMessage = consumerMessages.get(0);
    Assert.assertEquals(ControlChannel.MessageType.PRODUCER_ERROR, consumerMessage.getType());
    Assert.assertSame(producerError, consumerMessage.getPayload());
    controlChannel.producerError(producerError);
    Assert.assertSame(producerError, getError(take(true)));
    Assert.assertSame(producerError, getError(take(true)));
  }

  private Throwable getError(Future future) throws InterruptedException {
    try {
      future.get();
      throw new AssertionError("Future failed to throw expected exception");
    } catch (ExecutionException ex) {
      Throwable result = ex;
      while (result.getCause() != null) {
        result = result.getCause();
      }
      return result;
    }
  }

  private Future<List<Map.Entry>> take(final boolean commit) throws Exception {
    return executorService.submit(new Callable<List<Map.Entry>>() {
      @Override
      public List<Map.Entry> call() throws Exception {
        OffsetAndResult<Map.Entry> result = consumer.take();
        if (result == null) {
          throw new NullPointerException("result");
        }
        if (commit) {
          consumer.commit(String.valueOf(result.getOffset()));
        }
        return result.getResult();
      }
    });
  }
  private Future<?> put(final int size) throws Exception {
    return executorService.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        producer.put(new OffsetAndResult<>("123", createBatch(size)));
        producer.waitForCommit();
        return null;
      }
    });

  }

  private List<Map.Entry> createBatch(int size) {
    List<Map.Entry> batch = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      batch.add(new Record(i));
    }
    return batch;
  }

  private static class Record implements Map.Entry {
    private int i;

    Record(int i) {
      this.i = i;
    }
    @Override
    public Object getKey() {
      return "key-" + i;
    }

    @Override
    public Object getValue() {
      return "val-" + i;
    }

    @Override
    public Object setValue(Object value) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Record record = (Record) o;
      return i == record.i;
    }
    @Override
    public int hashCode() {
      return i;
    }
  }
}
