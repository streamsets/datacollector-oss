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
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;

public class MessageReceiverImpl implements MessageReceiver {
  private static final Logger LOG = LoggerFactory.getLogger(MessageReceiverImpl.class);

  private final SynchronousQueue<MessageReplyConsumerBundle> messages;

  public MessageReceiverImpl(SynchronousQueue<MessageReplyConsumerBundle> messages) { // NOSONAR
    LOG.info("Created new MessageReceiver");
    this.messages = messages;
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    try {
      messages.put(new MessageReplyConsumerBundle(message, consumer));
    } catch (InterruptedException e) {
      LOG.warn(
          "Thread interrupted while trying to enqueue message with id '{}'. Sending nack. Message will be re-received",
          message.getMessageId()
      );
      consumer.nack();
      Thread.currentThread().interrupt();
    }
  }
}
