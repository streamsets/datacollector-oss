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
package com.streamsets.pipeline.stage.origin.mysql;

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
import com.streamsets.pipeline.stage.connection.mysqlbinlog.MySQLBinLogConnection;
import com.streamsets.pipeline.stage.connection.mysqlbinlog.MySQLBinLogConnectionGroups;

import java.util.List;

@StageDef(
    version = 1,
    label = "MySQL Binary Log Connection Verifier",
    description = "Verifies a connection to a MySQL server",
    upgraderDef = "upgrader/MySQLBinLogConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(MySQLBinLogConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = MySQLBinLogConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class MySQLBinLogConnectionVerifier extends ConnectionVerifier {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = MySQLBinLogConnection.TYPE,
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
  public MySQLBinLogConnection connection;

  private DataSourceInitializer initializer;

  @Override
  protected List<ConfigIssue> initConnection() {
    // We will just use default values for some parameters which are not included in the connection itself.
    MySQLBinLogConfig config = new MySQLBinLogConfig();
    config.connectTimeout = 5000;
    config.serverId = "999";

    initializer = new DataSourceInitializer(
        "",
        connection,
        "",
        config,
        new ConfigIssueFactory(getContext())
    );
    return initializer.issues;
  }

  @Override
  protected void destroyConnection() {
    if (initializer != null) {
      initializer.destroy();
    }
  }
}
