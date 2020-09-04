/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class DatabricksConfig {

  public static final String DEFAULT_CLUSTER_CONFIG =
      "{\n    \"num_workers\": 8,\n    \"spark_version\": \"5.3.x-scala2.11\",\n    \"node_type_id\": \"i3.xlarge\"\n}";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "URL to connect to Databricks",
      group = "CLUSTER",
      displayPosition = 105,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  public String baseUrl = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Credential Type",
      defaultValue = "TOKEN",
      description = "Type of credential to use to connect to Databricks",
      displayPosition = 106,
      group = "CLUSTER",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  @ValueChooserModel(CredentialTypeChooserValues.class)
  public CredentialType credentialType = CredentialType.TOKEN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Databricks user name",
      displayPosition = 107,
      group = "CLUSTER",
      dependsOn = "credentialType",
      triggeredByValue = "PASSWORD",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password for the account",
      displayPosition = 108,
      group = "CLUSTER",
      dependsOn = "credentialType",
      triggeredByValue = "PASSWORD",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  public CredentialValue password = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Token",
      description = "Personal access token for the account",
      displayPosition = 109,
      group = "CLUSTER",
      dependsOn = "credentialType",
      triggeredByValue = "TOKEN",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  public CredentialValue token = () -> "";

  @ConfigDef(
      required = true,
      defaultValue = "true",
      type = ConfigDef.Type.BOOLEAN,
      label = "Provision a New Cluster",
      description = "Provisions a new cluster when the pipeline starts",
      group = "CLUSTER",
      displayPosition = 110,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          )
      }
  )
  public boolean provisionNewCluster = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster ID",
      group = "CLUSTER",
      displayPosition = 111,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          ),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "false")
      }
  )
  public String clusterId = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Cluster Configuration",
      description = "Configuration for the Databricks cluster",
      defaultValue = DEFAULT_CLUSTER_CONFIG,
      group = "CLUSTER",
      displayPosition = 112,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          ),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String clusterConfig = DEFAULT_CLUSTER_CONFIG;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "FALSE",
      label = "Terminate Cluster",
      description = "Terminates the cluster when the pipeline stops",
      group = "CLUSTER",
      displayPosition = 113,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"DATABRICKS"}
          ),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public boolean terminateCluster;

}
