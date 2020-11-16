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
package com.streamsets.pipeline.lib.jms.config.connection;

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
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jms.config.InitialContextFactory;
import com.streamsets.pipeline.lib.jms.config.JmsErrors;
import com.streamsets.pipeline.lib.jms.config.JmsGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@StageDef(
    version = 1,
    label = "JMS Connection Verifier",
    description = "Verifies connection to JMS",
    upgraderDef = "upgrader/JmsConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(JmsConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = JmsConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class JmsConnectionVerifier extends ConnectionVerifier {

  private final static Logger LOG = LoggerFactory.getLogger(JmsConnection.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = JmsConnection.TYPE,
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
  public JmsConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {

    // See JmsTarget:init for a full implementation.
    List<ConfigIssue> issues = new ArrayList<>();
    InitialContextFactory initialContextFactory = new InitialContextFactory();
    ConnectionFactory connectionFactory = null;
    InitialContext initialContext = null;
    Connection jmsConnection = null;

    try {
      Properties contextProperties = new Properties();
      contextProperties.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, connection.initialContextFactory);
      contextProperties.setProperty(javax.naming.Context.PROVIDER_URL, connection.providerURL);

      if (connection.initialContextFactory.toLowerCase(Locale.ENGLISH).contains("oracle")) {
        contextProperties.setProperty("db_url", connection.providerURL);
      }

      for (SecurityPropertyBean props : connection.additionalSecurityProps) {
        contextProperties.put(props.key, props.value.get());
      }

      initialContext = initialContextFactory.create(contextProperties);

    } catch (NamingException ex) {
      LOG.info(
          Utils.format(
              JmsErrors.JMS_00.getMessage(),
              connection.initialContextFactory,
              connection.providerURL,
              ex.toString()
          ),
          ex
      );
      issues.add(
          getContext().createConfigIssue(
              JmsGroups.JMS.name(),
              "connection.initialContextFactory",
              JmsErrors.JMS_00,
              connection.initialContextFactory,
              connection.providerURL,
              ex.toString()
          )
      );
    }

    if (issues.isEmpty()) {
      try {
        connectionFactory = (ConnectionFactory) initialContext.lookup(connection.connectionFactory);
      } catch (NamingException ex) {
        LOG.info(
            Utils.format(JmsErrors.JMS_01.getMessage(),
                connection.initialContextFactory,
                ex.toString()
            ), ex);
        issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), "connection.initialContextFactory", JmsErrors.JMS_01,
            connection.connectionFactory, ex.toString()));
      }
    }

    if (issues.isEmpty()) {
      try {
        if (connection.useCredentials) {
          jmsConnection = connectionFactory.createConnection(connection.username.get(), connection.password.get());
        } else {
          jmsConnection = connectionFactory.createConnection();
        }
        jmsConnection.close();
      } catch (JMSException ex) {
        if (connection.useCredentials) {
          issues.add(getContext().createConfigIssue(
              JmsGroups.JMS.name(),
              "connection.connectionFactory",
              JmsErrors.JMS_03,
              connectionFactory.getClass().getName(),
              ex.toString()
          ));
          LOG.info(Utils.format(JmsErrors.JMS_03.getMessage(), connectionFactory.getClass().getName(), ex.toString()), ex);
        } else {
          issues.add(getContext().createConfigIssue(JmsGroups.JMS.name(), "connection.connectionFactory", JmsErrors.JMS_02,
              connectionFactory.getClass().getName(), ex.toString()));
          LOG.info(Utils.format(JmsErrors.JMS_02.getMessage(), connectionFactory.getClass().getName(), ex.toString())
              , ex);
        }
      }
    }

    return issues;
  }

}
