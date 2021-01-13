/*
 * Copyright 2021 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jdbc.connection;

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
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.connection.common.JdbcConnectionGroups;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@StageDef(
    version = 1,
    label = "Postgres Connection Verifier",
    description = "Verifies connections for Postgres",
    upgraderDef = "upgrader/PostgresConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(JdbcConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = PostgresConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class PostgresConnectionVerifier extends ConnectionVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresConnection.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = PostgresConnection.TYPE,
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
  public PostgresConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (!DatabaseVendor.forUrl(connection.connectionString).equals(DatabaseVendor.POSTGRESQL)) {
      LOG.debug(JdbcErrors.JDBC_503.getMessage(), PostgresConnection.TYPE);
      issues.add(getContext().createConfigIssue("JDBC", "connection", JdbcErrors.JDBC_503, PostgresConnection.TYPE));
    } else {
      Properties connectionProps = new Properties();
      if (connection.useCredentials) {
        connectionProps.put("user", connection.username.get());
        connectionProps.put("password", connection.password.get());
      }

      try (Connection conn = DriverManager.getConnection(connection.connectionString, connectionProps)) {
        LOG.debug("Successfully connected to the database at {}", connection.connectionString);
      } catch (Exception e) {
        LOG.debug(JdbcErrors.JDBC_00.getMessage(), connection.connectionString, e.getMessage(), e);
        issues.add(getContext().createConfigIssue("JDBC", "connection", JdbcErrors.JDBC_00, e.toString(), e));
      }
    }
    return issues;
  }
}
