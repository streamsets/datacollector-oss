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

package com.streamsets.pipeline.stage.origin.pulsar;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

/**
 * Utility class for consuming Records to a Pulsar topic.
 */
public interface PulsarMessageConsumer {

  /**
   * Performs initial setup of Pulsar consumers based on provided configuration options
   *
   * @param context Invoking Stage's context
   * @return List of issues encountered during setup. If empty, operation was successful
   */
  List<Stage.ConfigIssue> init(Source.Context context);

  /**
   * Retrieves a list of messages from Pulsar Consumers
   *
   * @param batchMaker Pulsar messages batch creator
   * @param context Invoking Stage's context
   * @param batchSize Maximum and desired number of messages to put in the batch.
   * @return The number of consumed messages
   *
   * @throws StageException
   */
  int take(BatchMaker batchMaker, Source.Context context, int batchSize) throws StageException;

  /**
   * Acknowledges the messages received from Apache Pulsar since last ack was called
   *
   * @throws StageException
   */
  void ack() throws StageException;

  /**
   * Closes Pulsar Consumers
   */
  void close();

}
