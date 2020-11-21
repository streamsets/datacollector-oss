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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.pulsar.config.PulsarSecurityConfig;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.pulsar.PulsarTargetConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSourceConfig;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSubscriptionInitialPosition;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarSubscriptionType;
import com.streamsets.pipeline.stage.origin.pulsar.PulsarTopicsSelector;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
    pulsarTargetConfig.maxPendingMessages = 1000;
    pulsarTargetConfig.asyncSend = false;
    pulsarTargetConfig.enableBatching = false;
    pulsarTargetConfig.batchMaxMessages = 2000;
    pulsarTargetConfig.batchMaxPublishDelay = 1000;
    return pulsarTargetConfig;
  }

  public static PulsarSourceConfig getSourceConfig() {
    PulsarSourceConfig pulsarSourceConfig = new PulsarSourceConfig();
    pulsarSourceConfig.serviceURL = "http://localhost:8080";
    pulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.SINGLE_TOPIC;
    pulsarSourceConfig.patternAutoDiscoveryPeriod = 1;
    pulsarSourceConfig.priorityLevel = 0;
    pulsarSourceConfig.readCompacted = false;
    pulsarSourceConfig.originTopic = "sdc-topic";
    pulsarSourceConfig.consumerName = "sdc-consumer";
    pulsarSourceConfig.topicsList = Collections.emptyList();
    pulsarSourceConfig.subscriptionName = "sdc-test-subscription";
    pulsarSourceConfig.securityConfig = new PulsarSecurityConfig();
    pulsarSourceConfig.securityConfig.tlsEnabled = false;
    pulsarSourceConfig.securityConfig.tlsAuthEnabled = false;
    pulsarSourceConfig.subscriptionType = PulsarSubscriptionType.EXCLUSIVE;
    pulsarSourceConfig.subscriptionInitialPosition = PulsarSubscriptionInitialPosition.LATEST;
    pulsarSourceConfig.topicsPattern = "persistent://public/default/.*";
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
    recordsList.add(createRecord("value1"));
    recordsList.add(createRecord("value2"));
    recordsList.add(createRecord("value3"));
    recordsList.add(createRecord("value4"));
    recordsList.add(createRecord("value5"));
    return recordsList;
  }

  public static Record createRecord(String value) {
    Record record = RecordCreator.create();
    record.set(Field.create(value));
    return record;
  }

  public static Batch getBatch() {
    return new Batch() {
      @Override
      public String getSourceEntity() {
        return "sourceEntity";
      }

      @Override
      public String getSourceOffset() {
        return "sourceOffset";
      }

      @Override
      public Iterator<Record> getRecords() {
        return getRecordsList().iterator();
      }
    };
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
    return new MessageImpl<byte[]>(messageId, messageId, new HashMap<>(), Unpooled.wrappedBuffer(payload), null, PulsarApi.MessageMetadata.newBuilder());
  }

}
