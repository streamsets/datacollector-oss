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
package com.streamsets.transformer.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.aws.AwsInstanceType;
import com.streamsets.pipeline.lib.aws.AwsInstanceTypeChooserValues;
import com.streamsets.pipeline.lib.aws.AwsRegion;
import com.streamsets.pipeline.lib.aws.AwsRegionChooserValues;

import static com.streamsets.datacollector.config.AmazonEMRConfig.JOB_FLOW_ROLE_DEFAULT;
import static com.streamsets.datacollector.config.AmazonEMRConfig.SERVICE_ROLE_DEFAULT;

public class AmazonEMRConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Region",
      description = "AWS region",
      group = "EMR",
      displayPosition = 100,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR")
      }
  )
  @ValueChooserModel(AwsRegionChooserValues.class)
  public AwsRegion userRegion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "AWS user region (Custom)",
      group = "EMR",
      dependsOn = "userRegion",
      triggeredByValue = "OTHER",
      displayPosition = 105,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR")
      }
  )
  public String userRegionCustom;

  @ConfigDef(
    required = true,
    label = "Use IAM Roles",
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    description = "Use IAM roles instead of AWS credentials to connect to AWS",
    displayPosition = 108,
    group = "EMR"
  )
  public boolean useIAMRoles;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "AWS access key",
      group = "EMR",
      displayPosition = 110,
      dependencies = {
        @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
        @Dependency(configName = "useIAMRoles", triggeredByValues = "false")
  }
  )
  public CredentialValue accessKey = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "AWS secret key",
      group = "EMR",
      displayPosition = 120,
      dependencies = {
        @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
        @Dependency(configName = "useIAMRoles", triggeredByValues = "false")
      }
  )
  public CredentialValue secretKey = () -> "";

  @ConfigDef(
      required = true,
      defaultValue = "",
      type = ConfigDef.Type.STRING,
      label = "S3 Staging URI",
      description =
          "S3 Location where the SDC configuration and resources will be uploaded for the execution of the pipeline",
      group = "EMR",
      displayPosition = 140,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
      }
  )
  public String s3StagingUri;

  @ConfigDef(
      required = true,
      defaultValue = "false",
      type = ConfigDef.Type.BOOLEAN,
      label = "Provision a New Cluster",
      description = "Provisions a new cluster when the pipeline starts",
      group = "EMR",
      displayPosition = 150,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR")
      }
  )
  public boolean provisionNewCluster = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster ID",
      group = "EMR",
      displayPosition = 160,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "false")
      }
  )
  public String clusterId = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "EMR Version",
    group = "EMR",
    displayPosition = 190,
    dependencies = {
      @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
      @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
    }
  )
  public String emrVersion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster Name Prefix",
      group = "EMR",
      displayPosition = 200,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String clusterPrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Terminate Cluster",
      description = "Terminates the cluster when the pipeline stops",
      group = "EMR",
      displayPosition = 210,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public boolean terminateCluster;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Logging Enabled",
      group = "EMR",
      description = "Copy cluster log files to S3",
      displayPosition = 220,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public boolean loggingEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "S3 Log URI",
      group = "EMR",
      displayPosition = 230,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "loggingEnabled", triggeredByValues = "true"),
      }
  )
  public String s3LogUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = SERVICE_ROLE_DEFAULT,
      label = "Service Role",
      description = "EMR role used by the cluster when provisioning resources and performing other service-level " +
          "tasks",
      group = "EMR",
      displayPosition = 260,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String serviceRole;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = JOB_FLOW_ROLE_DEFAULT,
      label = "Job Flow Role",
      description = "EMR role for EC2 used by EC2 instances within the cluster",
      group = "EMR",
      displayPosition = 270,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String jobFlowRole;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Visible to All Users",
      group = "EMR",
      displayPosition = 280,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public boolean visibleToAllUsers;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "EC2 subnet ID",
      description = "EC2 subnet identifier to launch the cluster in",
      group = "EMR",
      displayPosition = 290,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String ec2SubnetId;

  // lets mandate security group for master as Transformer should be able to reach master for connecting to YARN
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master Security Group",
      description = "Security group ID for the master node",
      group = "EMR",
      displayPosition = 300,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String masterSecurityGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Slave Security Group",
      description = "Security group ID for the slave nodes",
      group = "EMR",
      displayPosition = 310,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String slaveSecurityGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2",
      label = "Instance Count",
      group = "EMR",
      displayPosition = 320,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public int instanceCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Master Instance Type",
      group = "EMR",
      displayPosition = 330,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  @ValueChooserModel(AwsInstanceTypeChooserValues.class)
  public AwsInstanceType masterInstanceType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master Instance type (Custom)",
      group = "EMR",
      displayPosition = 340,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "masterInstanceType", triggeredByValues = "OTHER")
      }
  )
  public String masterInstanceTypeCustom;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Slave Instance Type",
      group = "EMR",
      displayPosition = 350,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  @ValueChooserModel(AwsInstanceTypeChooserValues.class)
  public AwsInstanceType slaveInstanceType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Slave Instance type (Custom)",
      group = "EMR",
      displayPosition = 360,
      dependencies = {
          @Dependency(configName = "^clusterConfig.clusterType", triggeredByValues = "EMR"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "slaveInstanceType", triggeredByValues = "OTHER")
      }
  )
  public String slaveInstanceTypeCustom;

  public String getUserRegion() {
    if (userRegion != AwsRegion.OTHER) {
      return userRegion.getId();
    } else {
      return userRegionCustom;
    }
  }

  public String getMasterInstanceType() {
    if (masterInstanceType != null && masterInstanceType!= AwsInstanceType.OTHER) {
      return masterInstanceType.getId();
    } else {
      return masterInstanceTypeCustom;
    }
  }

  public String getSlaveInstanceType() {
    if (slaveInstanceType != null && slaveInstanceType != AwsInstanceType.OTHER) {
      return slaveInstanceType.getId();
    } else {
      return slaveInstanceTypeCustom;
    }
  }
}
