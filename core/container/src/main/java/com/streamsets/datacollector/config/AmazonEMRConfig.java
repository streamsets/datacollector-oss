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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.Properties;

public class AmazonEMRConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Region",
      description = "AWS region",
      group = "EMR",
      displayPosition = 100,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH")
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
      triggeredByValue = "CUSTOM",
      displayPosition = 105,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH")
      }
  )
  public String userRegionCustom;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "AWS access key",
      group = "EMR",
      displayPosition = 110,
      dependencies = {
      @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH")
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH")
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH")
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "false")
      }
  )
  public String clusterId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster Name Prefix",
      group = "EMR",
      displayPosition = 200,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String clusterPrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "FALSE",
      label = "Terminate Cluster",
      description = "Terminates the cluster when the pipeline stops",
      group = "EMR",
      displayPosition = 210,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public boolean terminateCluster;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "TRUE",
      label = "Logging Enabled",
      group = "EMR",
      description = "Copy cluster log files to S3",
      displayPosition = 220,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "loggingEnabled", triggeredByValues = "true"),
      }
  )
  public String s3LogUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "TRUE",
      label = "Enable Debugging",
      description = "Enable console debugging in EMR",
      group = "EMR",
      displayPosition = 240,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "loggingEnabled", triggeredByValues = "true"),

      }
  )
  public boolean enableEMRDebugging;

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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true")
      }
  )
  public String ec2SubnetId;

  // lets mandate security group for master as SDC should be able to reach master for connecting to YARN
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master Security Group",
      description = "Security group ID for the master node",
      group = "EMR",
      displayPosition = 300,
      dependencies = {
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "masterInstanceType", triggeredByValues = "CUSTOM")
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
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
          @Dependency(configName = "^executionMode", triggeredByValues = "EMR_BATCH"),
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "slaveInstanceType", triggeredByValues = "CUSTOM")
      }
  )
  public String slaveInstanceTypeCustom;

  public String getUserRegion() {
    if (userRegion != AwsRegion.CUSTOM) {
      return userRegion.getId();
    } else {
      return userRegionCustom;
    }
  }

  public String getMasterInstanceType() {
    if (masterInstanceType != null && masterInstanceType!= AwsInstanceType.CUSTOM) {
      return masterInstanceType.getId();
    } else {
      return masterInstanceTypeCustom;
    }
  }

  public String getSlaveInstanceType() {
    if (slaveInstanceType != null && slaveInstanceType != AwsInstanceType.CUSTOM) {
      return slaveInstanceType.getId();
    } else {
      return slaveInstanceTypeCustom;
    }
  }

  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String USER_REGION = "userRegion";
  public static final String USER_REGION_CUSTOM = "userRegionCustom";
  public static final String S3_STAGING_URI = "s3StagingUri";
  public static final String PROVISION_NEW_CLUSTER ="provisionNewCluster";
  public static final String CLUSTER_PREFIX = "clusterPrefix";
  public static final String CLUSTER_ID = "clusterId";
  public static final String TERMINATE_CLUSTER = "terminateCluster";
  public static final String SERVICE_ROLE = "serviceRole";
  public static final String JOB_FLOW_ROLE = "jobFlowRole";
  public static final String EC2_SUBNET_ID = "ec2SubnetId";
  public static final String MASTER_SECURITY_GROUP = "masterSecurityGroup";
  public static final String SLAVE_SECURITY_GROUP = "slaveSecurityGroup";
  public static final String INSTANCE_COUNT = "instanceCount";
  public static final String MASTER_INSTANCE_TYPE = "masterInstanceType";
  public static final String MASTER_INSTANCE_TYPE_CUSTOM = "masterInstanceTypeCustom";
  public static final String SLAVE_INSTANCE_TYPE = "slaveInstanceType";
  public static final String SLAVE_INSTANCE_TYPE_CUSTOM = "slaveInstanceTypeCustom";
  public static final String ENABLE_EMR_DEBUGGING = "enableEMRDebugging";
  public static final String S3_LOG_URI = "s3LogUri";
  public static final String SERVICE_ROLE_DEFAULT = "EMR_DefaultRole";
  public static final String JOB_FLOW_ROLE_DEFAULT = "EMR_EC2_DefaultRole";
  public static final String VISIBLE_TO_ALL_USERS = "visibleToAllUsers";
  public static final String LOGGING_ENABLED = "loggingEnabled";


  public Properties convertToProperties() throws StageException {
    Properties props = new Properties();
    props.setProperty(ACCESS_KEY, accessKey.get());
    props.setProperty(SECRET_KEY, secretKey.get());
    props.setProperty(USER_REGION, getUserRegion());
    props.setProperty(S3_STAGING_URI, s3StagingUri);
    props.setProperty(PROVISION_NEW_CLUSTER, Boolean.toString(provisionNewCluster));
    props.setProperty(CLUSTER_PREFIX, clusterPrefix);
    props.setProperty(CLUSTER_ID, clusterId);
    props.setProperty(SERVICE_ROLE, serviceRole);
    props.setProperty(JOB_FLOW_ROLE, jobFlowRole);
    props.setProperty(EC2_SUBNET_ID, ec2SubnetId);
    props.setProperty(TERMINATE_CLUSTER, Boolean.toString(terminateCluster));
    props.setProperty(MASTER_SECURITY_GROUP, masterSecurityGroup);
    props.setProperty(SLAVE_SECURITY_GROUP, slaveSecurityGroup);
    props.setProperty(INSTANCE_COUNT, Integer.toString(instanceCount));
    props.setProperty(MASTER_INSTANCE_TYPE, getMasterInstanceType());
    props.setProperty(SLAVE_INSTANCE_TYPE, getSlaveInstanceType());
    props.setProperty(ENABLE_EMR_DEBUGGING, Boolean.toString(enableEMRDebugging));
    props.setProperty(VISIBLE_TO_ALL_USERS, Boolean.toString(visibleToAllUsers));
    props.setProperty(S3_LOG_URI, s3LogUri);
    props.setProperty(LOGGING_ENABLED, Boolean.toString(loggingEnabled));
    return props;
  }

}
