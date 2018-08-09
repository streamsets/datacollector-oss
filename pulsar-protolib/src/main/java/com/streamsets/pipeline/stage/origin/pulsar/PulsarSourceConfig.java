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
      label = "Pulsar Topics Pattern",
      description = "Pattern used to select the list of Pulsar topics from which messages have to be retrieved. " +
          "Example: persistent://public/default/sdc-.* would match topics like 'sdc-topic' or 'sdc-data'",
      displayPosition = 70,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "sdc-.*",
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
      description = "List of Pulsar topics from which messages have to be retrieved",
      displayPosition = 70,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
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
      description = "Pulsar topic from which messages have to be retrieved",
      displayPosition = 70,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
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

  @ConfigDef(required = false,
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
      label = "Subscription Initial Position",
      description = "Initial position from which to start reading Pulsar messages",
      displayPosition = 30,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarSubscriptionInitialPositionChooserValues.class)
  public PulsarSubscriptionInitialPosition subscriptionInitialPosition;

  @ConfigDef(required = false,
      type = ConfigDef.Type.NUMBER,
      label = "Pattern Auto Discovery Period",
      description = "Set topics auto discovery period when using a pattern to specify topics to consume from. The " +
          "period is in minutes",
      displayPosition = 40,
      defaultValue = "1",
      min = 1,
      dependencies = {
          @Dependency(configName = "pulsarTopicsSelector",
              triggeredByValues = "TOPICS_PATTERN"),
      },
      group = "ADVANCED")
  public int patternAutoDiscoveryPeriod;

}
