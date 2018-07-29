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

package com.streamsets.pipeline.stage.pubsub.lib;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.common.base.Throwables;
import com.google.pubsub.v1.PubsubMessage;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestMessageReceiverImpl {
  @Test
  public void receiveMessage() throws Exception {
    SynchronousQueue<MessageReplyConsumerBundle> queue = new SynchronousQueue<>();
    MessageReceiver messageReceiver = new MessageReceiverImpl(queue);

    PubsubMessage message = PubsubMessage.getDefaultInstance();
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);

    CompletableFuture<MessageReplyConsumerBundle> future = CompletableFuture.supplyAsync(() -> {
      MessageReplyConsumerBundle bundle = null;
      try {
        bundle = queue.take();
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
      return bundle;
    });

    messageReceiver.receiveMessage(message, consumer);

    MessageReplyConsumerBundle bundle = future.get();
    Assert.assertNotNull(bundle);
    Assert.assertEquals(message, bundle.getMessage());
    Assert.assertEquals(consumer, bundle.getConsumer());
  }

  @Test
  public void interruptReceive() throws Exception {
    SynchronousQueue<MessageReplyConsumerBundle> queue = new SynchronousQueue<>();
    MessageReceiver messageReceiver = new MessageReceiverImpl(queue);

    PubsubMessage message = PubsubMessage.newBuilder().setMessageId("1234").build();
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);

    Thread t = new Thread(() -> messageReceiver.receiveMessage(message, consumer));
    t.start();
    t.interrupt();
    ThreadUtil.sleep(50);
    verify(consumer, times(1)).nack();
  }
}