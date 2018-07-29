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
package com.streamsets.datacollector.util;

import java.util.Properties;

public class EmrClusterConfig {
  static final String ACCESS_KEY = "accessKey";
  private static final String SECRET_KEY = "secretKey";
  private static final String USER_REGION = "userRegion";
  private static final String S3_STAGING_URI = "s3StagingUri";
  private static final String PROVISION_NEW_CLUSTER ="provisionNewCluster";
  private static final String CLUSTER_PREFIX = "clusterPrefix";
  private static final String SERVICE_ROLE = "serviceRole";
  private static final String JOB_FLOW_ROLE = "jobFlowRole";
  private static final String EC2_SUBNET_ID = "ec2SubnetId";
  private static final String MASTER_SECURITY_GROUP = "masterSecurityGroup";
  private static final String SLAVE_SECURITY_GROUP = "slaveSecurityGroup";
  private static final String INSTANCE_COUNT = "instanceCount";
  private static final String MASTER_INSTANCE_TYPE = "masterInstanceType";
  private static final String SLAVE_INSTANCE_TYPE = "slaveInstanceType";
  private static final String ENABLE_EMR_DEBUGGING = "enableEMRDebugging";
  private static final String S3_LOG_URI = "s3LogUri";
  private static final String CLUSTER_ID = "clusterId";
  private static final String VISIBLE_TO_ALL_USERS = "visibleToAllUsers";
  private static final String TERMINATE_CLUSTER = "terminateCluster";
  private static final String LOGGING_ENABLED = "loggingEnabled";


  Properties props;

  public EmrClusterConfig(Properties props) {
    this.props = props;
  }

  public String getAccessKey() {
    return props.getProperty(ACCESS_KEY);
  }

  public boolean isVisibleToAllUsers() {
    return Boolean.parseBoolean(props.getProperty(VISIBLE_TO_ALL_USERS));
  }

  public String getSecretKey() {
    return props.getProperty(SECRET_KEY);
  }

  public String getUserRegion() {
    return props.getProperty(USER_REGION);
  }

  public String getS3StagingUri() {
    return props.getProperty(S3_STAGING_URI);
  }

  public boolean isLoggingEnabled() {
    return Boolean.parseBoolean(props.getProperty(LOGGING_ENABLED));
  }

  public boolean isProvisionNewCluster() {
    return Boolean.parseBoolean(props.getProperty(PROVISION_NEW_CLUSTER));
  }

  public String getClusterPrefix() {
    return props.getProperty(CLUSTER_PREFIX);
  }

  public String getClusterId() {
    return props.getProperty(CLUSTER_ID);
  }

  public String getServiceRole() {
    return props.getProperty(SERVICE_ROLE);
  }

  public boolean isTerminateCluster() {
    return Boolean.parseBoolean(props.getProperty(TERMINATE_CLUSTER));
  }

  public String getJobFlowRole() {
    return props.getProperty(JOB_FLOW_ROLE);
  }

  public String getEc2SubnetId() {
    return props.getProperty(EC2_SUBNET_ID);
  }

  public String getMasterSecurityGroup() {
    return props.getProperty(MASTER_SECURITY_GROUP);
  }

  public String getSlaveSecurityGroup() {
    return props.getProperty(SLAVE_SECURITY_GROUP);
  }

  public int getInstanceCount() {
    return Integer.parseInt(props.getProperty(INSTANCE_COUNT));
  }

  public String getMasterInstanceType() {
    return props.getProperty(MASTER_INSTANCE_TYPE);
  }

  public String getSlaveInstanceType() {
    return props.getProperty(SLAVE_INSTANCE_TYPE);
  }

  public boolean isEnableEmrDebugging() {
    return Boolean.parseBoolean(props.getProperty(ENABLE_EMR_DEBUGGING));
  }

  public String getS3LogUri() {
    return props.getProperty(S3_LOG_URI);
  }

}
