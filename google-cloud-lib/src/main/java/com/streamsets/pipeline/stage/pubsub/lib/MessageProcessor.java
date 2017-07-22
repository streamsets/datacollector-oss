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

import java.util.concurrent.Callable;

/**
 * Interface for receiving {@link MessageReplyConsumerBundle} handling the parsing of the message data and attributes
 * and finally enqueuing a reply (usually ack) upon successfully completing a batch.
 *
 * There is a {@link MessageProcessor} instantiated for each thread in
 * {@link com.streamsets.pipeline.stage.pubsub.origin.PubSubSourceConfig#maxThreads}
 */
public interface MessageProcessor extends Callable<Void> {
  /**
   * Signals that the {@link MessageProcessor} should stop accepting new messages and begin shutting down.
   */
  void stop();
}
