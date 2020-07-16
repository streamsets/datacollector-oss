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

public class ClusterConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Cluster Manager Type",
      description = "Type of cluster manager used by the Spark cluster",
      group = "CLUSTER",
      defaultValue = "LOCAL",
      displayPosition = 101,
      dependencies = {
          @Dependency(
              configName = "^executionMode",
              triggeredByValues = {"BATCH", "STREAMING"}
           )
      }
  )
  @ValueChooserModel(SparkClusterTypeChooserValues.class)
  public SparkClusterType clusterType = SparkClusterType.LOCAL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master URL",
      description = "Master URL to connect to Spark",
      group = "CLUSTER",
      defaultValue = "local[*]",
      displayPosition = 102,
      dependsOn = "clusterType",
      triggeredByValue = {"LOCAL", "STANDALONE_SPARK_CLUSTER"}
  )
  public String sparkMasterUrl = "local[*]";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Deployment Mode",
      description = "Mode used to launch the Spark driver process",
      group = "CLUSTER",
      defaultValue = "CLIENT",
      displayPosition = 103,
      dependsOn = "clusterType",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"YARN", "STANDALONE_SPARK_CLUSTER", "KUBERNETES"}
  )
  @ValueChooserModel(SparkDeployModeChooserValues.class)
  public SparkDeployMode deployMode = SparkDeployMode.CLIENT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Hadoop User Name",
      description = "Name of the Hadoop user that StreamSets impersonates",
      group = "CLUSTER",
      defaultValue = "",
      displayPosition = 104,
      dependsOn = "clusterType",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = "YARN"
  )
  public String hadoopUserName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Application Name",
      description = "Name of the launched Spark application",
      group = "CLUSTER",
      defaultValue = "${pipeline:title()}",
      displayPosition = 105,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(
              configName = "^executionMode",
              triggeredByValues = {"BATCH", "STREAMING"}
          )
      }
  )
  public String sparkAppName = "${pipeline:title()}" ;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Staging Directory",
      description = "Staging directory on the remote system for copying StreamSets resources",
      group = "CLUSTER",
      defaultValue = "/streamsets",
      displayPosition = 106,
      dependsOn = "clusterType",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      triggeredByValue = {"DATABRICKS", "SQL_SERVER_BIG_DATA_CLUSTER", "AZURE_HD_INSIGHT", "EMR"}
  )
  public String stagingDir = "/streamsets";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use YARN Kerberos Keytab",
      description = "Indicates that the Spark application should use a principal and keytab for Kerberos authentication",
      group = "CLUSTER",
      defaultValue = "NONE",
      displayPosition = 1000,
      dependsOn = "clusterType",
      triggeredByValue = "YARN"
  )
  public boolean useYarnKerberosKeytab;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Keytab Source",
      description = "Source for the Kerberos keytab used to launch the Spark application",
      group = "CLUSTER",
      defaultValue = "PROPERTIES_FILE",
      displayPosition = 1050,
      dependsOn = "useYarnKerberosKeytab",
      triggeredByValue = "true"
  )
  @ValueChooserModel(KeytabSourceChooserValues.class)
  public KeytabSource yarnKerberosKeytabSource = KeytabSource.PROPERTIES_FILE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "YARN Kerberos Keytab Path",
      description = "Absolute path to the Kerberos keytab used to launch the Spark application for the pipeline",
      group = "CLUSTER",
      displayPosition = 1100,
      dependsOn = "yarnKerberosKeytabSource",
      triggeredByValue = "PIPELINE"
  )
  public String yarnKerberosKeytab;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Keytab Credential Function",
      description = "Credential function to retrieve the base64 encoded keytab from a credential store.",
      displayPosition = 1150,
      dependsOn = "yarnKerberosKeytabSource",
      triggeredByValue = "PIPELINE_CREDENTIAL_STORE",
      group = "CLUSTER",
      upload = ConfigDef.Upload.BASE64
  )
  public CredentialValue yarnKerberosKeytabBase64Bytes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "YARN Kerberos Principal",
      description = "Name of the Kerberos principal used to launch the Spark application for the pipeline.  Must be" +
          " present in the keytab specified above.",
      group = "CLUSTER",
      defaultValue = "name@DOMAIN",
      displayPosition = 1200,
      dependsOn = "yarnKerberosKeytabSource",
      triggeredByValue = {"PIPELINE", "PIPELINE_CREDENTIAL_STORE"}
  )
  public String yarnKerberosPrincipal = "name@DOMAIN";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Cluster Callback URL",
      description = "Optional callback URL for the Spark cluster to use to contact this Transformer instance. " +
          "Use this format: http://<hostname>:<port>",
      displayPosition = 5,
      group = "ADVANCED",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependencies = {
          @Dependency(
              configName = "^executionMode",
              triggeredByValues = {"BATCH", "STREAMING"}
          )
      }
  )
  public String callbackUrl;

}
