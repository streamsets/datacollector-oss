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

public class EMRJobConfig {

  private static final String STEP_ID = "stepId";
  private static final String JOB_NAME = "jobName";
  private static final String PIPELINE_ID = "pipelineId";
  private static final String UNIQUE_PREFIX = "uniquePrefix";
  private static final String DRIVER_JAR_PATH = "driverJarPath";
  private static final String DRIVER_MAIN_CLASS = "driverMainClass";
  private static final String ARCHIVES = "archives";
  private static final String LIBJARS = "libjars";
  private static final String CLUSTER_ID = "clusterId";
  private static final String JAVA_OPTS = "javaopts";
  private static final String LOG_LEVEL = "logLevel";

  private Properties props;

  public EMRJobConfig(Properties props) {
    this.props = props;
  }

  public String getStepId() {
    return props.getProperty(STEP_ID);
  }

  public String getJavaOpts() {
    return props.getProperty(JAVA_OPTS);
  }

  public String getClusterId() {
    return props.getProperty(CLUSTER_ID);
  }

  public String getJobName() {
    return props.getProperty(JOB_NAME);
  }

  public String getPipelineId() {
    return props.getProperty(PIPELINE_ID);
  }

  public String getUniquePrefix() {
    return props.getProperty(UNIQUE_PREFIX);
  }

  public String getDriverJarPath() {
    return props.getProperty(DRIVER_JAR_PATH);
  }

  public String getDriverMainClass() {
    return props.getProperty(DRIVER_MAIN_CLASS);
  }

  public String getArchives() {
    return props.getProperty(ARCHIVES);
  }

  public String getLibjars() {
    return props.getProperty(LIBJARS);
  }

  public String getLogLevel() {
    return props.getProperty(LOG_LEVEL);
  }

}
