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

package com.streamsets.pipeline.stage.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.BrandedHikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;

import java.util.List;
import java.util.Properties;

public class SapHanaHikariPoolConfigBean extends BrandedHikariPoolConfigBean {

  private static final String JDBC_SAP_CONNECTION_WITH_PORT_STRING_TEMPLATE = "jdbc:sap://%s:%d/?databaseName=%s";
  public static final String SPLIT_BATCH_COMMANDS = "splitBatchCommands";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Host",
      description = "IP address or host name where to access the database",
      displayPosition = 7,
      group = "JDBC"
  )
  public String hostSAP = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Port",
      description = "Port to access the database",
      displayPosition = 8,
      group = "JDBC"
  )
  public int portSAP = -1;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Database",
      description = "Database name",
      displayPosition = 9,
      group = "JDBC"
  )
  public String databaseNameSAP = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Split Batch Commands",
      description = "Allows split and parallel execution of batch commands on partitioned tables",
      displayPosition = 1,
      group = "SAP_HANA"
  )
  public boolean splitBatchCommandsSAP = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Include SAP HANA Connection Details",
      description = "Includes SAP HANA connection details in a SapHANA record header",
      displayPosition = 2,
      group = "SAP_HANA"
  )
  public boolean pullClientInfoSAP = false;

  @Override
  public String getConnectionString() {
    return String.format(JDBC_SAP_CONNECTION_WITH_PORT_STRING_TEMPLATE, hostSAP, portSAP, databaseNameSAP);
  }

  public boolean isSplitBatchCommandsSAP(){
    return splitBatchCommandsSAP;
  }

  @Override
  public Properties getDriverProperties() throws StageException {
    Properties driverProperties = super.getDriverProperties();
    if(isSplitBatchCommandsSAP()){
      driverProperties.setProperty(SPLIT_BATCH_COMMANDS, "TRUE");
    }else{
      driverProperties.remove(SPLIT_BATCH_COMMANDS);
    }
    return driverProperties;
  }

  @Override
  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    List<Stage.ConfigIssue> configIssues = super.validateConfigs(context, issues);
    return configIssues;
  }
}
