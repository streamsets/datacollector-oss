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
import org.apache.pulsar.client.api.Message;

import java.util.List;

/**
 * Utility class for converting Pulsar messages to datacollector records
 */
public interface PulsarMessageConverter {

  /**
   * Performs initial setup of Pulsar converter based on provided configuration options
   * @param context Invoking Stage's context
   * @return List of issues encountered during setup. If empty, operation was successful
   */
  List<Stage.ConfigIssue> init(Source.Context context);

  /**
   * Converts a Pulsar message to a datacollector Records and adds it to the batch that will be sent to next stage
   * @param batchMaker The container of Records belonging to the batch to be sent to next stage
   * @param context Invoking Stage's context
   * @param messageId The Identifier of the message
   * @param message The Pulsar message to be converted
   * @return The number of records added to the batchMaker
   */
  int convert(BatchMaker batchMaker, Source.Context context, String messageId, Message message) throws StageException;

}
