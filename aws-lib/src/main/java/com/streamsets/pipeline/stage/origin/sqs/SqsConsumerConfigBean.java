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
package com.streamsets.pipeline.stage.origin.sqs;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.common.sqs.AwsSqsConnection;

import java.util.List;

public class SqsConsumerConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = AwsSqsConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = -500
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public AwsSqsConnection connection;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Specify Queue URL directly",
      description = "Allows specifying URL instead of prefix which disables validation of existence.",
      displayPosition = 105,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.BASIC)
  public boolean specifyQueueURL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Queue Name Prefixes",
      description = "The name prefixes of queues to fetch messages from. All unique queue names having these prefixes" +
          " will be assigned to all available threads in a round-robin fashion.",
      displayPosition = 110,
      dependsOn = "specifyQueueURL",
      triggeredByValue = "false",
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.BASIC)
  public List<String> queuePrefixes;

  @ConfigDef(required = true,
      type = ConfigDef.Type.LIST,
      label = "Queue URLs",
      description = "The URLs of the queues",
      displayPosition = 110,
      dependsOn = "specifyQueueURL",
      triggeredByValue = "true",
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.BASIC)
  public List<String> queueUrls;

  @ConfigDef(required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Number of Messages per Request",
      description = "The max number of messages that should be retrieved per request.",
      displayPosition = 120,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1
  )
  public int numberOfMessagesPerRequest;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      description = "Max number of records per batch.",
      displayPosition = 160,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Wait Time (ms)",
      description = "The maximum time a partial batch will remain open before being flushed (milliseconds).",
      displayPosition = 170,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public long maxBatchTimeMs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${runtime:availableProcessors()}",
      label = "Max Threads",
      description = "Maximum number of record processing threads to spawn.",
      displayPosition = 190,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Poll Wait Time (Seconds)",
      description = "Maximum length of time to wait for messages to arrive when polling before returning" +
          " an empty response. Provides long polling functionality. Leave as -1 to not set parameter (and" +
          " thus not use long polling).",
      displayPosition = 190,
      group = "SQS",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = -1,
      max = 20
  )
  public int pollWaitTimeSeconds;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BASIC",
      label = "SQS Message Attribute Level",
      description = "Level of SQS message metadata and attributes to include as SDC record attributes.",
      displayPosition = 200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SQS"
  )
  @ValueChooserModel(SqsAttributesOptionChooserValues.class)
  public SqsAttributesOption sqsAttributesOption;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      label = "Include SQS Sender Attributes",
      description = "Required for any sender attributes that you want to include as record header attributes.",
      displayPosition = 210,
      dependsOn = "sqsAttributesOption",
      triggeredByValue = "ALL",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "SQS"
  )
  public List<String> sqsMessageAttributeNames;

}
