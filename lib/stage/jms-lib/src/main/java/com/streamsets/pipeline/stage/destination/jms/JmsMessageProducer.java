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
package com.streamsets.pipeline.stage.destination.jms;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.List;

/**
 * Utility class for producing Records to a JMS queue or topic.
 */
public interface JmsMessageProducer {
  /**
   * Performs initial setup of JMS connections and sessions based on provided configuration options
   * @param context Invoking Stage's context
   * @return List of issues encountered during setup. If empty, operation was successful
   */
  List<Stage.ConfigIssue> init(Target.Context context);

  /**
   * Converts Batch of Records to appropriate JMS Message type and produces message to configured destination.
   * @param batch Batch of Records to produce
   * @return Number of Records successfully produced
   * @throws StageException
   */
  int put(Batch batch) throws StageException;

  /**
   * Commits transactional JMS Session to perform pending produce operation
   * @throws StageException
   */
  void commit() throws StageException;

  /**
   * Discards pending produce operations
   * @throws StageException
   */
  void rollback() throws StageException;

  /**
   * Closes JMS connections and sessions
   */
  void close();
}
