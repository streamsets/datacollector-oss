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
  public SparkClusterType clusterType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master URL",
      description = "Master URL to connect to Spark",
      group = "CLUSTER",
      defaultValue = "local[*]",
      displayPosition = 102,
      dependsOn = "clusterType",
      triggeredByValue = "LOCAL"
  )
  public String sparkMasterUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Deployment Mode",
      description = "Mode used to launch the Spark driver process",
      group = "CLUSTER",
      defaultValue = "CLIENT",
      displayPosition = 103,
      dependsOn = "clusterType",
      triggeredByValue = "YARN"
  )
  @ValueChooserModel(SparkDeployModeChooserValues.class)
  public SparkDeployMode deployMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Hadoop User Name",
      description = "Name of the Hadoop user that StreamSets impersonates",
      group = "CLUSTER",
      defaultValue = "hdfs",
      displayPosition = 104,
      dependsOn = "clusterType",
      triggeredByValue = "YARN"
  )
  @ValueChooserModel(SparkDeployModeChooserValues.class)
  public String hadoopUserName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Application Name",
      description = "Name of the launched Spark application",
      group = "CLUSTER",
      defaultValue = "${pipeline:title()}",
      displayPosition = 105,
      dependencies = {
          @Dependency(
              configName = "^executionMode",
              triggeredByValues = {"BATCH", "STREAMING"}
          )
      }
  )
  public String sparkAppName;

}
