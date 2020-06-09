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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.pulsar.config.BasePulsarConfig;

import java.util.List;

public class PulsarSourceConfig extends BasePulsarConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Subscription Name",
      description = "The name of the Pulsar subscription that will forward messages from the corresponding Pulsar " +
          "topic to subscribed consumers. Pulsar maintains an offset for each subscription to remember from where it " +
          "shall start reading messages",
      displayPosition = 20,
      defaultValue = "sdc-subscription",
      group = "PULSAR"
  )
  public String subscriptionName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Consumer Name",
      description = "The name to be assigned to the Pulsar Consumer",
      displayPosition = 50,
      group = "PULSAR"
  )
  public String consumerName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SINGLE_TOPIC",
      label = "Topics Selector",
      description = "Selector to indicate how topics to consume from are specified",
      displayPosition = 60,
      group = "PULSAR"
  )
  @ValueChooserModel(PulsarTopicsSelectorChooserValues.class)
  public PulsarTopicsSelector pulsarTopicsSelector;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topics Pattern",
      description = "Pattern used to select the list of Pulsar topics from which messages have to be retrieved. " +
          "Pattern format: {persistent|non-persistent}://<tenant>/<namespace>/<topic pattern> Example: " +
          "persistent://public/default/sdc-.* would match topics like 'sdc-topic' or 'sdc-data'",
      displayPosition = 70,
      defaultValue = "persistent://public/default/sdc-.*",
      dependencies = {
          @Dependency(configName = "pulsarTopicsSelector",
              triggeredByValues = "TOPICS_PATTERN"),
      },
      group = "PULSAR"
  )
  public String topicsPattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Topics List",
      description = "List of Pulsar topics from which messages have to be retrieved. Each topic should have next " +
          "format: {persistent|non-persistent}://<tenant>/<namespace>/<topic name>",
      displayPosition = 70,
      dependencies = {
          @Dependency(configName = "pulsarTopicsSelector",
              triggeredByValues = "TOPICS_LIST"),
      },
      group = "PULSAR"
  )
  public List<String> topicsList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topic",
      description = "Pulsar topic from which messages have to be retrieved. Topic should have next format: " +
          "{persistent|non-persistent}://<tenant>/<namespace>/<topic name>",
      displayPosition = 70,
      dependencies = {
          @Dependency(configName = "pulsarTopicsSelector",
              triggeredByValues = "SINGLE_TOPIC"),
      },
      group = "PULSAR"
  )
  public String originTopic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "EXCLUSIVE",
      label = "Subscription Type",
      description = "Type of subscription used when subscribing the Pulsar Consumer to the corresponding topic",
      displayPosition = 10,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarSubscriptionTypeChooserValues.class)
  public PulsarSubscriptionType subscriptionType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Consumer Queue Size",
      description = "Size assigned to the queue where messages are pre-fetched until application asks Pulsar for them",
      displayPosition = 20,
      defaultValue = "1000",
      min = 1, // 0 not allowed as 0 does not allow to use Consumer.receive(int, TimeUnit) nor partitioned topics
      group = "ADVANCED")
  public int receiverQueueSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LATEST",
      label = "Initial Offset",
      description = "Initial offset from which to start reading Pulsar messages",
      displayPosition = 30,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarSubscriptionInitialPositionChooserValues.class)
  public PulsarSubscriptionInitialPosition subscriptionInitialPosition;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Pattern Auto Discovery Period (minutes)",
      description = "Topics auto discovery period when using a pattern to specify topics to consume from",
      displayPosition = 40,
      defaultValue = "1",
      min = 1,
      dependencies = {
          @Dependency(configName = "pulsarTopicsSelector",
              triggeredByValues = "TOPICS_PATTERN"),
      },
      group = "ADVANCED")
  public int patternAutoDiscoveryPeriod;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Consumer Priority Level",
      description = "The level of priority that will be assigned to pulsar consumer. One consumer will receive more " +
          "messages than other with less priority where 0=max-priority",
      displayPosition = 50,
      defaultValue = "0",
      min = 0,
      max = 100,
      group = "ADVANCED")
  public int priorityLevel;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Read Compacted",
      description = "If enabled, the consumer will read messages from the compacted topic rather than reading the " +
          "full message backlog of the topic. This means that, if the topic has been compacted, the consumer will " +
          "only see the latest value for each key in the topic, up until the point in the topic message backlog " +
          "that has been compacted. Note Read Compacted can only be enabled with persistent topics",
      displayPosition = 60,
      defaultValue = "false",
      dependencies = {
          @Dependency(configName = "subscriptionType",
              triggeredByValues = {"EXCLUSIVE", "FAILOVER"}),
      },
      group = "ADVANCED")
  public boolean readCompacted;

}
