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

package com.streamsets.pipeline.stage.Utils;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.pulsar.config.PulsarSecurityConfig;
import com.streamsets.pipeline.stage.destination.pulsar.PulsarTargetConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSourceConfig;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSubscriptionInitialPosition;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSubscriptionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TestUtilsPulsar {

  public static PulsarTargetConfig getTargetConfig() {
    PulsarTargetConfig pulsarTargetConfig = new PulsarTargetConfig();
    pulsarTargetConfig.serviceURL = "http://localhost:8080";
    pulsarTargetConfig.destinationTopic = "sdc-topic";
    pulsarTargetConfig.securityConfig = new PulsarSecurityConfig();
    pulsarTargetConfig.securityConfig.tlsEnabled = false;
    pulsarTargetConfig.securityConfig.tlsAuthEnabled = false;
    pulsarTargetConfig.messageKey = null;
    return pulsarTargetConfig;
  }

  public static PulsarSourceConfig getSourceConfig() {
    PulsarSourceConfig pulsarSourceConfig = new PulsarSourceConfig();
    pulsarSourceConfig.serviceURL = "http://localhost:8080";
    pulsarSourceConfig.multiTopic = false;
    pulsarSourceConfig.originTopic = "sdc-topic";
    pulsarSourceConfig.consumerName = "sdc-consumer";
    pulsarSourceConfig.topicsList = Collections.emptyList();
    pulsarSourceConfig.subscriptionName = "sdc-test-subscription";
    pulsarSourceConfig.securityConfig = new PulsarSecurityConfig();
    pulsarSourceConfig.securityConfig.tlsEnabled = false;
    pulsarSourceConfig.securityConfig.tlsAuthEnabled = false;
    pulsarSourceConfig.subscriptionType = PulsarSubscriptionType.EXCLUSIVE;
    pulsarSourceConfig.subscriptionInitialPosition = PulsarSubscriptionInitialPosition.LATEST;
    return pulsarSourceConfig;
  }

  public static BasicConfig getBasicConfig() {
    BasicConfig basicConfig = new BasicConfig();
    basicConfig.maxWaitTime = 2000; // time in ms
    basicConfig.maxBatchSize = 1000; // number of records
    return basicConfig;
  }

  public static MessageConfig getMessageConfig() {
    MessageConfig messageConfig = new MessageConfig();
    messageConfig.produceSingleRecordPerMessage = false;
    return messageConfig;
  }

  public static List<Stage.ConfigIssue> getStageConfigIssues() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    ConfigIssue issue = new ConfigIssue() {};
    issues.add(issue);

    return issues;
  }

  public static List<ConfigIssue> getConfigIssues() {
    List<ConfigIssue> issues = new ArrayList<>();

    ConfigIssue issue = new ConfigIssue() {};
    issues.add(issue);

    return issues;
  }

  public static List<Record> getRecordsList() {
    List<Record> recordsList = new ArrayList<>();

    recordsList.add(new RecordImpl(null, Field.create("value1")));
    recordsList.add(new RecordImpl(null, Field.create("value2")));
    recordsList.add(new RecordImpl(null, Field.create("value3")));
    recordsList.add(new RecordImpl(null, Field.create("value4")));
    recordsList.add(new RecordImpl(null, Field.create("value5")));

    return recordsList;
  }

  public static Batch getBatch() {
    return new BatchImpl("instanceName", "sourceEntity", "sourceOffset", getRecordsList());
  }

  public static List<String> getTopicsList() {
    List<String> topicsList = new ArrayList<>();

    topicsList.add("sdc-topic1");
    topicsList.add("sdc-topic2");
    topicsList.add("sdc-topic3");

    return topicsList;
  }

  public static Message getPulsarMessage() {
    return getPulsarMessage(new byte[]{0x01, 0x02, 0x03});
  }

  public static Message getPulsarMessage(byte[] payload) {
    return getPulsarMessage("1286:0:-1:0", payload);
  }

  public static Message getPulsarMessage(String messageId, byte[] payload) {
    return new MessageImpl(messageId, new HashMap<>(), payload, null);
  }

}
