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

import com.streamsets.datacollector.config.LogLevel;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.delegate.exported.ClusterJob;
import com.streamsets.pipeline.lib.aws.AwsInstanceType;
import com.streamsets.pipeline.stage.common.emr.EMRClusterConnection;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class TestEmrClusterProvider {

  private PipelineConfigBean getAwsPipelineConfigBean(String clusterId) {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    pipelineConfigBean.sdcEmrConnection = new EMRClusterConnection();
    pipelineConfigBean.sdcEmrConnection.provisionNewCluster = false;
    pipelineConfigBean.sdcEmrConnection.clusterId = clusterId;
    pipelineConfigBean.sdcEmrConnection.region = AwsRegion.AP_NORTHEAST_1;
    pipelineConfigBean.sdcEmrConnection.instanceCount = 1;
    pipelineConfigBean.sdcEmrConnection.masterInstanceType = AwsInstanceType.M4_2XLARGE;
    pipelineConfigBean.sdcEmrConnection.slaveInstanceType = AwsInstanceType.M4_2XLARGE;
    pipelineConfigBean.enableEMRDebugging = true;
    pipelineConfigBean.sdcEmrConnection.emrVersion = "Something random";
    pipelineConfigBean.sdcEmrConnection.s3StagingUri = "";
    pipelineConfigBean.sdcEmrConnection.clusterPrefix = "";
    pipelineConfigBean.sdcEmrConnection.serviceRole = "";
    pipelineConfigBean.sdcEmrConnection.jobFlowRole = "";
    pipelineConfigBean.sdcEmrConnection.ec2SubnetId = "";
    pipelineConfigBean.sdcEmrConnection.masterSecurityGroup = "";
    pipelineConfigBean.sdcEmrConnection.slaveSecurityGroup = "";
    pipelineConfigBean.sdcEmrConnection.s3LogUri = "";
    pipelineConfigBean.logLevel = LogLevel.DEBUG;
    pipelineConfigBean.clusterSlaveJavaOpts = "foo";
    pipelineConfigBean.clusterSlaveMemory = 10;
    return pipelineConfigBean;
  }

  @Test
  public void testKillPipeline() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    SecurityConfiguration securityConfiguration  = Mockito.mock(SecurityConfiguration.class);
    Configuration conf = Mockito.mock(Configuration.class);
    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    ClusterJob clusterJob = Mockito.mock(ClusterJob.class);
    ClusterJob.Client client = Mockito.mock(ClusterJob.Client.class);
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    PipelineConfigBean pipelineConfigBean = getAwsPipelineConfigBean("111");

    EmrClusterProvider emrClusterProvider = Mockito.spy(new EmrClusterProvider(runtimeInfo, securityConfiguration, conf,
        stageLibraryTask));
    Mockito.doReturn(clusterJob).when(emrClusterProvider).getClusterJobDelegator(Mockito.any());
    Mockito.doReturn(client).when(clusterJob).getClient(Mockito.any());
    Properties properties = new Properties();
    properties.setProperty("appId", "foo");
    Mockito.doReturn(properties).when(client).getJobStatus(Mockito.any());
    Mockito.doNothing().when(client).terminateJob(Mockito.any());
    ApplicationState applicationState = new ApplicationState();
    applicationState.setEmrConfig(new Properties());
    emrClusterProvider.killPipeline(
        Mockito.mock(File.class),
        applicationState,
        pipelineConfiguration,
        pipelineConfigBean
    );
    Mockito.verify(client, Mockito.times(1)).terminateJob(Mockito.any());
  }

  @Test
  public void testGetStatus() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    SecurityConfiguration securityConfiguration = Mockito.mock(SecurityConfiguration.class);
    Configuration conf = Mockito.mock(Configuration.class);
    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    ClusterJob clusterJob = Mockito.mock(ClusterJob.class);
    ClusterJob.Client client = Mockito.mock(ClusterJob.Client.class);
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    PipelineConfigBean pipelineConfigBean = getAwsPipelineConfigBean("111");

    EmrClusterProvider emrClusterProvider = Mockito.spy(new EmrClusterProvider(runtimeInfo,
        securityConfiguration,
        conf,
        stageLibraryTask
    ));
    Mockito.doReturn(clusterJob).when(emrClusterProvider).getClusterJobDelegator(Mockito.any());
    Mockito.doReturn(client).when(clusterJob).getClient(Mockito.any());


    Properties properties = new Properties();
    properties.setProperty("clusterId", "111");
    ApplicationState applicationState = new ApplicationState();
    applicationState.setEmrConfig(properties);

    properties.setProperty("state", "TERMINATED");
    Mockito.doReturn(properties).when(client).getClusterStatus(Mockito.eq(properties.getProperty("clusterId")));
    emrClusterProvider.getStatus(Mockito.mock(File.class), applicationState, pipelineConfiguration, pipelineConfigBean);
    Mockito.verify(client, Mockito.times(1)).getClusterStatus(Mockito.eq(properties.getProperty("clusterId")));

    properties.setProperty("state", "RUNNING");
    Mockito.doReturn(properties).when(client).getClusterStatus(Mockito.eq(properties.getProperty("clusterId")));
    Mockito.doReturn(properties).when(client).getJobStatus(Mockito.any());
    emrClusterProvider.getStatus(Mockito.mock(File.class), applicationState, pipelineConfiguration, pipelineConfigBean);
    Mockito.verify(client, Mockito.times(2)).getClusterStatus(Mockito.eq(properties.getProperty("clusterId")));
    Mockito.verify(client, Mockito.times(1)).getJobStatus(Mockito.any());
  }

  @Test
  public void testCleanUp() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    SecurityConfiguration securityConfiguration = Mockito.mock(SecurityConfiguration.class);
    Configuration conf = Mockito.mock(Configuration.class);
    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    ClusterJob clusterJob = Mockito.mock(ClusterJob.class);
    ClusterJob.Client client = Mockito.mock(ClusterJob.Client.class);
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    PipelineConfigBean pipelineConfigBean = getAwsPipelineConfigBean("111");

    EmrClusterProvider emrClusterProvider = Mockito.spy(new EmrClusterProvider(runtimeInfo,
        securityConfiguration,
        conf,
        stageLibraryTask
    ));
    Mockito.doReturn(clusterJob).when(emrClusterProvider).getClusterJobDelegator(Mockito.any());
    Mockito.doReturn(client).when(clusterJob).getClient(Mockito.any());


    Properties properties = new Properties();
    properties.setProperty("clusterId", "111");
    ApplicationState applicationState = new ApplicationState();
    applicationState.setEmrConfig(properties);
    pipelineConfigBean.sdcEmrConnection.provisionNewCluster = true;
    pipelineConfigBean.sdcEmrConnection.terminateCluster = true;
    Mockito.doNothing().when(client).terminateCluster(Mockito.eq(properties.getProperty("clusterId")));
    Mockito.doNothing().when(client).deleteJobFiles(Mockito.any());
    emrClusterProvider.cleanUp(applicationState, pipelineConfiguration, pipelineConfigBean);

    Mockito.verify(client, Mockito.times(1)).terminateCluster(Mockito.eq(properties.getProperty("clusterId")));
    Mockito.verify(client, Mockito.times(1)).deleteJobFiles(Mockito.any());
  }

  @Test
  public void testStartPipelineExecute() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    SecurityConfiguration securityConfiguration  = Mockito.mock(SecurityConfiguration.class);
    Configuration conf = Mockito.mock(Configuration.class);
    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    PipelineConfigBean pipelineConfigBean = getAwsPipelineConfigBean("10");

    ClusterJob clusterJob = Mockito.mock(ClusterJob.class);
    ClusterJob.Client client = Mockito.mock(ClusterJob.Client.class);


    EmrClusterProvider emrClusterProvider = Mockito.spy(new EmrClusterProvider(runtimeInfo, securityConfiguration, conf,
        stageLibraryTask));
    Mockito.doReturn(clusterJob).when(emrClusterProvider).getClusterJobDelegator(Mockito.any());
    Mockito.doReturn(client).when(clusterJob).getClient(Mockito.any());

    Properties properties = new Properties();
    properties.setProperty("clusterId", pipelineConfigBean.sdcEmrConnection.clusterId);
    properties.setProperty("libjars", "resource1,resource2");
    properties.setProperty("archives", "resource1,resource2");
    properties.setProperty("stepId", "stepId");
    properties.setProperty("driverMainClass", "com.streamsets.pipeline.BootstrapEmrBatch");

    Mockito.doReturn(properties).when(client).submitJob(Mockito.any());
    Mockito.doReturn(Arrays.asList("resource1", "resource2")).when(client).uploadJobFiles(Mockito.any(), Mockito.any());


    Mockito.doReturn("pipelineId").when(pipelineConfiguration).getPipelineId();
    Mockito.doReturn("pipelineTitle").when(pipelineConfiguration).getTitle();
    Mockito.doNothing().when(emrClusterProvider).copyFile(Mockito.any(File.class), Mockito.any(File.class));
    Mockito.doNothing().when(emrClusterProvider).replaceFileInJar(Mockito.any(), Mockito.any());
    File mockFile = Mockito.mock(File.class);
    Mockito.doReturn("foo").when(mockFile).getName();
    Mockito.doReturn("foo").when(mockFile).getPath();
    mockFile = File.createTempFile(getClass().getSimpleName(), "");

    ApplicationState applicationState = emrClusterProvider.startPipelineExecute(
        mockFile,
        Collections.emptyMap(),
        pipelineConfiguration,
        pipelineConfigBean,
        -1,
        mockFile,
        "",
        mockFile,
        mockFile,
        Collections.emptySet(),
        mockFile,
        mockFile,
        mockFile,
        mockFile,
        mockFile,
        "",
        "",
        "",
        "",
        Collections.emptyList()
    );
    Assert.assertNotNull(applicationState);
    Assert.assertNotNull(applicationState.getEmrConfig());
    Assert.assertEquals(pipelineConfigBean.sdcEmrConnection.clusterId, applicationState.getEmrConfig().getProperty
        ("clusterId"));
    Assert.assertEquals("stepId", applicationState.getEmrConfig().getProperty
        ("stepId"));
    Assert.assertEquals("resource1,resource2", applicationState.getEmrConfig().getProperty
        ("libjars"));
    Assert.assertEquals("resource1,resource2", applicationState.getEmrConfig().getProperty
        ("archives"));
    Assert.assertEquals("com.streamsets.pipeline.BootstrapEmrBatch", applicationState.getEmrConfig().getProperty
        ("driverMainClass"));

  }

}
