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
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.origin.kinesis.DataFormatChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;

public class SqsConsumerConfigBean {

  @ConfigDefBean(groups = "SQS")
  public AWSConfig awsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Region",
      displayPosition = 100,
      group = "SQS"
  )
  @ValueChooserModel(AWSRegionChooserValues.class)
  public AWSRegions region;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = 100,
      dependsOn = "region",
      triggeredByValue = "OTHER",
      group = "SQS"
  )
  public String endpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Queue Name Prefixes",
      description = "The name prefixes of queues to fetch messages from. All unique queue names having these prefixes" +
          " will be assigned to all available threads in a round-robin fashion.",
      displayPosition = 110,
      group = "SQS"
  )
  public List<String> queuePrefixes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Number of Messages per Request",
      description = "The max number of messages that should be retrieved per request.",
      displayPosition = 120,
      group = "SQS",
      min = 1
  )
  public int numberOfMessagesPerRequest;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig;

  @ConfigDefBean(groups = "ADVANCED")
  public ProxyConfig proxyConfig = new ProxyConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when receiving records from SQS",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      description = "Max number of records per batch.",
      displayPosition = 160,
      group = "SQS",
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
      group = "SQS"
  )
  public List<String> sqsMessageAttributeNames;

}
