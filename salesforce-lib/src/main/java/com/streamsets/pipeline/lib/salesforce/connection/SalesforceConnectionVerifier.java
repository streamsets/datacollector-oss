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

package com.streamsets.pipeline.lib.salesforce.connection;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.ConnectionException;
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
import com.streamsets.pipeline.lib.salesforce.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@StageDef(
    version = 1,
    label = "Salesforce Connection Verifier",
    description = "Verifies connection to Salesforce",
    upgraderDef = "upgrader/SalesforceConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(SalesforceConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = SalesforceConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class SalesforceConnectionVerifier extends ConnectionVerifier {

  private final static Logger LOG = LoggerFactory.getLogger(SalesforceConnection.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = SalesforceConnection.TYPE,
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
  public SalesforceConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    try {
      ConnectorConfig partnerConfig = new ConnectorConfig();
      partnerConfig.setUsername(connection.username.get());
      partnerConfig.setPassword(connection.password.get());
      partnerConfig.setAuthEndpoint("https://"+connection.authEndpoint+"/services/Soap/u/"+connection.apiVersion);

      if (connection.useProxy) {
        partnerConfig.setProxy(connection.proxyHostname, connection.proxyPort);
        if (connection.useProxyCredentials) {
          partnerConfig.setProxyUsername(connection.proxyUsername.get());
          partnerConfig.setProxyPassword(connection.proxyPassword.get());
        }
      }

      /*
        See example here:
          https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_code_walkthrough.htm
        PartnerConfig creates the connection and obtains a session ID that is added as an attribute to partnerConfig.
        If the login was successful, it is populated; otherwise, it stays empty.
      */
      new PartnerConnection(partnerConfig);
      if (partnerConfig.getSessionId().isEmpty()) {
        issues.add(
            getContext().createConfigIssue(
                "Salesforce",
                "connection",
                Errors.FORCE_47
            )
        );
        LOG.debug(Errors.FORCE_47.getMessage());
      } else {
        LOG.debug("Successfully authenticated as {}", connection.username);
      }
    } catch (Exception ex) {
      LOG.debug(Errors.FORCE_47.getMessage());
      issues.add(getContext().createConfigIssue(
          "Salesforce",
          "connection",
          Errors.FORCE_47
      ));
    }
    return issues;
  }

}
