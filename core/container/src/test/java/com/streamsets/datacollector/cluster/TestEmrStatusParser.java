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
package com.streamsets.datacollector.cluster;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TestEmrStatusParser {

  @Test
  public void testClusterStateParser() {
    Properties properties = new Properties();
    properties.setProperty("state", "BOOTSTRAPPING");
    Assert.assertEquals(ClusterPipelineStatus.STARTING, EmrStatusParser.parseClusterStatus(properties));
    properties.setProperty("state", "STARTING");
    Assert.assertEquals(ClusterPipelineStatus.STARTING, EmrStatusParser.parseClusterStatus(properties));
    properties.setProperty("state", "WAITING");
    Assert.assertEquals(ClusterPipelineStatus.RUNNING, EmrStatusParser.parseClusterStatus(properties));
    properties.setProperty("state", "RUNNING");
    Assert.assertEquals(ClusterPipelineStatus.RUNNING, EmrStatusParser.parseClusterStatus(properties));
    properties.setProperty("state", "TERMINATED");
    Assert.assertEquals(ClusterPipelineStatus.KILLED, EmrStatusParser.parseClusterStatus(properties));
  }

  @Test
  public void testJobStateParser() {
    Properties properties = new Properties();
    properties.setProperty("state", "PENDING");
    Assert.assertEquals(ClusterPipelineStatus.STARTING, EmrStatusParser.parseJobStatus(properties));
    properties.setProperty("state", "RUNNING");
    Assert.assertEquals(ClusterPipelineStatus.RUNNING, EmrStatusParser.parseJobStatus(properties));
    properties.setProperty("state", "COMPLETED");
    Assert.assertEquals(ClusterPipelineStatus.RUNNING, EmrStatusParser.parseJobStatus(properties));
    properties.setProperty("state", "NEW");
    Assert.assertEquals(ClusterPipelineStatus.RUNNING, EmrStatusParser.parseJobStatus(properties));
    properties.setProperty("state", "KILLED");
    Assert.assertEquals(ClusterPipelineStatus.KILLED, EmrStatusParser.parseJobStatus(properties));
    properties.setProperty("state", "SUCCEEDED");
    Assert.assertEquals(ClusterPipelineStatus.SUCCEEDED, EmrStatusParser.parseJobStatus(properties));
  }

}
