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

import com.streamsets.pipeline.stage.lib.aws.AWSCredentialMode;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import static com.streamsets.pipeline.stage.common.emr.EMRClusterConnection.*;

public class EmrClusterConfig {
  private static final String ENABLE_EMR_DEBUGGING = "enableEMRDebugging";

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

  public String getEMRVersion() {
    return props.getProperty(EMR_VERSION);
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

  public String getServiceAccessSecurityGroup() {
    return props.getProperty(SERVICE_ACCESS_SECURITY_GROUP);
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

  public int getStepConcurrency() {
    String steps = props.getProperty(STEP_CONCURRENCY);
    if (steps != null) {
      return Integer.parseInt(steps);
    } else {
      return 1;
    }
  }

  public AWSCredentialMode getAwsCredentialMode() {
    final String credentialModeStr = props.getProperty(AWS_CREDENTIAL_MODE);
    if (StringUtils.isNotBlank(credentialModeStr)) {
      return AWSCredentialMode.valueOf(credentialModeStr);
    } else {
      return null;
    }
  }

}
