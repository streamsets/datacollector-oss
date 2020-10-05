/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.lib.kafka.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.kafka.KafkaSecurityUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@StageDef(
    version = 1,
    label = "Kafka Connection Verifier",
    description = "Verifies connections for Kafka",
    upgraderDef = "upgrader/KafkaConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(KafkaConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = KafkaConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class KafkaConnectionVerifier extends ConnectionVerifier {

  private final static Logger LOG = LoggerFactory.getLogger(KafkaConnection.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = KafkaConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
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
  public KafkaConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", connection.metadataBrokerList);

    Map<String, String> securityProperties = new HashMap<>();
    KafkaSecurityUtil.addSecurityConfigs(connection.securityConfig, securityProperties);
    securityProperties.forEach(props::setProperty);

    try (AdminClient client = KafkaAdminClient.create(props)) {
      ListTopicsResult topics = client.listTopics();
      topics.names().get();
    } catch (Exception e) {
      LOG.debug(KafkaErrors.KAFKA_67.getMessage(), connection.metadataBrokerList, e.getMessage(), e);
      issues.add(getContext().createConfigIssue("KAFKA", "connection", KafkaErrors.KAFKA_67, e.toString(), e));
    }

    return issues;
  }
}
