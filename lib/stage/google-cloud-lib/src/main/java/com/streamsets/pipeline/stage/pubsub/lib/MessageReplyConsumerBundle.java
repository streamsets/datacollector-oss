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
import com.google.pubsub.v1.PubsubMessage;

/**
 * Wrapper class for a message and reply consumer to pass
 * to a Message Processor instance so that it can be
 * acknowledged only after an SDC batch is complete.
 */
public class MessageReplyConsumerBundle {
  private final PubsubMessage message;
  private final AckReplyConsumer consumer;

  public MessageReplyConsumerBundle(PubsubMessage message, AckReplyConsumer consumer) {
    this.message = message;
    this.consumer = consumer;
  }

  public PubsubMessage getMessage() {
    return message;
  }

  public AckReplyConsumer getConsumer() {
    return consumer;
  }
}
