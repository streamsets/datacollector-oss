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
package com.streamsets.pipeline.stage.common.emr;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.aws.AwsInstanceType;
import com.streamsets.pipeline.lib.aws.AwsInstanceTypeChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.aws.AwsRegionChooserValues;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Amazon EMR Cluster Manager",
    type = EMRClusterConnection.TYPE,
    description = "Connects to Amazon EMR",
    version = 2,
    upgraderDef = "upgrader/EMRClusterConnection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER}
)
@ConfigGroups(EMRClusterConnectionGroups.class)
@HideConfigs({"awsConfig.isAssumeRole"})
public class EMRClusterConnection {

  public static final String TYPE = "STREAMSETS_AWS_EMR_CLUSTER";

  public static final String SERVICE_ROLE_DEFAULT = "EMR_DefaultRole";
  public static final String JOB_FLOW_ROLE_DEFAULT = "EMR_EC2_DefaultRole";

  @ConfigDefBean()
  public AWSConfig awsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "AWS Region",
      description = "The AWS region for EMR.",
      displayPosition = 1000,
      group = "#0"
  )
  @ValueChooserModel(AwsRegionChooserValues.class)
  public AwsRegion region;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "Custom endpoint",
      group = "#0",
      dependsOn = "region",
      triggeredByValue = "OTHER",
      displayPosition = 1200
  )
  public String customRegion;

  @ConfigDef(
      required = true,
      defaultValue = "",
      type = ConfigDef.Type.STRING,
      label = "S3 Staging URI",
      description = "S3 URI where resources are staged for pipeline execution. Use the format: s3://<path>.",
      group = "#0",
      displayPosition = 1500
  )
  public String s3StagingUri;

  @ConfigDef(
      required = true,
      defaultValue = "false",
      type = ConfigDef.Type.BOOLEAN,
      label = "Provision a New Cluster",
      description = "Provisions a new cluster when the pipeline starts",
      group = "#0",
      displayPosition = 2000
  )
  public boolean provisionNewCluster = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster ID",
      group = "#0",
      displayPosition = 2500,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "false"
  )
  public String clusterId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "EMR Version",
      group = "#0",
      displayPosition = 3000,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String emrVersion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Cluster Name Prefix",
      group = "#0",
      displayPosition = 3100,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String clusterPrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Terminate Cluster",
      description = "Terminates the cluster when the pipeline stops",
      group = "#0",
      displayPosition = 3200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public boolean terminateCluster = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Logging Enabled",
      group = "#0",
      description = "Copies cluster log files to the specified S3 location. Use to enable continued access to log files.",
      displayPosition = 3300,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public boolean loggingEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "S3 Log URI",
      description = "S3 URI to store log files. Use the format: s3://<path>.",
      group = "#0",
      displayPosition = 3500,
      dependencies = {
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
      group = "#0",
      displayPosition = 4000,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String serviceRole;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = JOB_FLOW_ROLE_DEFAULT,
      label = "Job Flow Role",
      description = "EMR role for EC2. Used by EC2 instances within the cluster.",
      group = "#0",
      displayPosition = 4100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String jobFlowRole;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Visible to All Users",
      group = "#0",
      displayPosition = 4200,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public boolean visibleToAllUsers;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "EC2 Subnet ID",
      description = "ID of the EC2 subnet to launch the cluster in",
      group = "#0",
      displayPosition = 4500,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String ec2SubnetId;

  // lets mandate security group for master as Transformer should be able to reach master for connecting to YARN
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master Security Group",
      description = "ID of the security group for the master node",
      group = "#0",
      displayPosition = 4700,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String masterSecurityGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Slave Security Group",
      description = "ID of the security group for slave nodes",
      group = "#0",
      displayPosition = 4800,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String slaveSecurityGroup;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Service Access Security Group",
      description = "ID of the security group for the Amazon EMR service to access clusters in VPC private subnets",
      group = "#0",
      displayPosition = 4900,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public String serviceAccessSecurityGroup;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2",
      label = "Instance Count",
      description = "EC2 instances in the cluster. Minimum of 2. Adding instances for multiple partitions " +
          "can improve performance",
      group = "#0",
      min = 2,
      displayPosition = 5000,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public int instanceCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Step Concurrency",
      description = "Number of EMR steps this cluster can run concurrently",
      group = "#0",
      min = 1,
      displayPosition = 5000,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  public int stepConcurrency = 1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Master Instance Type",
      group = "#0",
      displayPosition = 7000,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  @ValueChooserModel(AwsInstanceTypeChooserValues.class)
  // TODO: dynamically query EC2 to determine these instead
  // https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instance-types.html
  public AwsInstanceType masterInstanceType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Master Instance Type (Custom)",
      group = "#0",
      displayPosition = 7100,
      dependencies = {
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "masterInstanceType", triggeredByValues = "OTHER")
      }
  )
  public String masterInstanceTypeCustom;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Slave Instance Type",
      group = "#0",
      displayPosition = 8000,
      dependsOn = "provisionNewCluster",
      triggeredByValue = "true"
  )
  @ValueChooserModel(AwsInstanceTypeChooserValues.class)
  public AwsInstanceType slaveInstanceType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Slave Instance Type (Custom)",
      group = "#0",
      displayPosition = 8100,
      dependencies = {
          @Dependency(configName = "provisionNewCluster", triggeredByValues = "true"),
          @Dependency(configName = "slaveInstanceType", triggeredByValues = "OTHER")
      }
  )
  public String slaveInstanceTypeCustom;

  public static final String AWS_CREDENTIAL_MODE = "awsCredentialMode";
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String USER_REGION = "userRegion";
  public static final String USER_REGION_CUSTOM = "userRegionCustom";
  public static final String S3_STAGING_URI = "s3StagingUri";
  public static final String PROVISION_NEW_CLUSTER ="provisionNewCluster";
  public static final String EMR_VERSION = "emrVersion";
  public static final String CLUSTER_PREFIX = "clusterPrefix";
  public static final String CLUSTER_ID = "clusterId";
  public static final String TERMINATE_CLUSTER = "terminateCluster";
  public static final String SERVICE_ROLE = "serviceRole";
  public static final String JOB_FLOW_ROLE = "jobFlowRole";
  public static final String EC2_SUBNET_ID = "ec2SubnetId";
  public static final String MASTER_SECURITY_GROUP = "masterSecurityGroup";
  public static final String SLAVE_SECURITY_GROUP = "slaveSecurityGroup";
  public static final String SERVICE_ACCESS_SECURITY_GROUP = "serviceAccessScurityGroup";
  public static final String INSTANCE_COUNT = "instanceCount";
  public static final String MASTER_INSTANCE_TYPE = "masterInstanceType";
  public static final String MASTER_INSTANCE_TYPE_CUSTOM = "masterInstanceTypeCustom";
  public static final String SLAVE_INSTANCE_TYPE = "slaveInstanceType";
  public static final String SLAVE_INSTANCE_TYPE_CUSTOM = "slaveInstanceTypeCustom";
  public static final String S3_LOG_URI = "s3LogUri";
  public static final String VISIBLE_TO_ALL_USERS = "visibleToAllUsers";
  public static final String LOGGING_ENABLED = "loggingEnabled";
  public static final String STEP_CONCURRENCY = "stepConcurrency";

  public String getUserRegion() {
    if (region != AwsRegion.OTHER) {
      return region.getId();
    } else {
      return customRegion;
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

  public Properties convertToProperties() throws StageException {
    Properties props = new Properties();
    if (awsConfig != null && awsConfig.awsAccessKeyId != null) {
      props.setProperty(ACCESS_KEY, awsConfig.awsAccessKeyId.get());
    }
    if (awsConfig != null && awsConfig.awsSecretAccessKey != null) {
      props.setProperty(SECRET_KEY, awsConfig.awsSecretAccessKey.get());
    }
    props.setProperty(USER_REGION, getUserRegion());
    props.setProperty(S3_STAGING_URI, s3StagingUri);
    props.setProperty(PROVISION_NEW_CLUSTER, Boolean.toString(provisionNewCluster));
    props.setProperty(EMR_VERSION, emrVersion);
    props.setProperty(CLUSTER_PREFIX, clusterPrefix);
    props.setProperty(CLUSTER_ID, clusterId);
    props.setProperty(SERVICE_ROLE, serviceRole);
    props.setProperty(JOB_FLOW_ROLE, jobFlowRole);
    props.setProperty(EC2_SUBNET_ID, ec2SubnetId);
    props.setProperty(TERMINATE_CLUSTER, Boolean.toString(terminateCluster));
    props.setProperty(MASTER_SECURITY_GROUP, masterSecurityGroup);
    props.setProperty(SLAVE_SECURITY_GROUP, slaveSecurityGroup);
    if (StringUtils.isNotBlank(serviceAccessSecurityGroup)) {
      props.setProperty(SERVICE_ACCESS_SECURITY_GROUP, serviceAccessSecurityGroup);
    }
    props.setProperty(INSTANCE_COUNT, Integer.toString(instanceCount));
    props.setProperty(MASTER_INSTANCE_TYPE, getMasterInstanceType());
    props.setProperty(SLAVE_INSTANCE_TYPE, getSlaveInstanceType());
    props.setProperty(VISIBLE_TO_ALL_USERS, Boolean.toString(visibleToAllUsers));
    props.setProperty(S3_LOG_URI, s3LogUri);
    props.setProperty(LOGGING_ENABLED, Boolean.toString(loggingEnabled));
    props.setProperty(STEP_CONCURRENCY, String.valueOf(stepConcurrency));
    if (awsConfig != null && awsConfig.credentialMode != null) {
      props.setProperty(
          AWS_CREDENTIAL_MODE,
          awsConfig.credentialMode.name()
      );
    }
    return props;
  }

}
