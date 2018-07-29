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
package com.streamsets.datacollector.lib.emr;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsResult;
import com.streamsets.datacollector.util.EmrClusterConfig;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

public class TestEmrClusterJob {

  @Test
  public void testCreateCluster() {
    Properties properties = new Properties();
    properties.setProperty("instanceCount", "1");

    EmrClusterJob emrClusterJob = new EmrClusterJob();
    EmrClusterJob.Client client = Mockito.spy(emrClusterJob.getClient(properties));
    AmazonElasticMapReduce emr = Mockito.mock(AmazonElasticMapReduce.class);
    Mockito.doReturn(Mockito.mock(RunJobFlowResult.class)).when(emr).runJobFlow(Mockito.any(RunJobFlowRequest.class));
    Mockito.doReturn(emr).when(client).getEmrClient(Mockito.any(EmrClusterConfig.class));
    client.createCluster("foo");
    Mockito.verify(emr, Mockito.times(1)).runJobFlow(Mockito.any(RunJobFlowRequest.class));
    Mockito.verify(client, Mockito.times(1)).getEmrClient(Mockito.any(EmrClusterConfig.class));

  }

  @Test
  public void testTerminateCluster() {
    Properties properties = new Properties();
    EmrClusterJob emrClusterJob = new EmrClusterJob();
    EmrClusterJob.Client client = Mockito.spy(emrClusterJob.getClient(properties));
    AmazonElasticMapReduce emr = Mockito.mock(AmazonElasticMapReduce.class);
    Mockito.doReturn(emr).when(client).getEmrClient(Mockito.any(EmrClusterConfig.class));
    Mockito.doReturn(Mockito.mock(TerminateJobFlowsResult.class)).when(emr).terminateJobFlows(Mockito.any(TerminateJobFlowsRequest
        .class));
    client.terminateCluster("foo");
    Mockito.verify(emr, Mockito.times(1)).terminateJobFlows(Mockito.any(TerminateJobFlowsRequest.class));
    Mockito.verify(client, Mockito.times(1)).getEmrClient(Mockito.any(EmrClusterConfig.class));

  }

  @Test
  public void testGetActiveCluster() {
    Properties properties = new Properties();
    EmrClusterJob emrClusterJob = new EmrClusterJob();
    EmrClusterJob.Client client = Mockito.spy(emrClusterJob.getClient(properties));
    AmazonElasticMapReduce emr = Mockito.mock(AmazonElasticMapReduce.class);
    Mockito.doReturn(emr).when(client).getEmrClient(Mockito.any(EmrClusterConfig.class));
    Mockito.doReturn(Mockito.mock(ListClustersResult.class)).when(emr).listClusters(Mockito.any(ListClustersRequest
        .class));
    client.getActiveCluster("foo");
    Mockito.verify(emr, Mockito.times(1)).listClusters(Mockito.any(ListClustersRequest.class));
    Mockito.verify(client, Mockito.times(1)).getEmrClient(Mockito.any(EmrClusterConfig.class));
  }

  @Test
  public void testGetClusterStatus() {
    Properties properties = new Properties();
    EmrClusterJob emrClusterJob = new EmrClusterJob();
    EmrClusterJob.Client client = Mockito.spy(emrClusterJob.getClient(properties));
    AmazonElasticMapReduce emr = Mockito.mock(AmazonElasticMapReduce.class);
    Mockito.doReturn(emr).when(client).getEmrClient(Mockito.any(EmrClusterConfig.class));
    DescribeClusterResult result = Mockito.mock(DescribeClusterResult.class);
    Mockito.doReturn(result).when(emr).describeCluster(Mockito.any(DescribeClusterRequest
        .class));
    Cluster cluster = Mockito.mock(Cluster.class);
    Mockito.doReturn(cluster).when(result).getCluster();
    Mockito.doReturn(Mockito.mock(ClusterStatus.class)).when(cluster).getStatus();
    client.getClusterStatus("foo");
    Mockito.verify(emr, Mockito.times(1)).describeCluster(Mockito.any(DescribeClusterRequest
        .class));
    Mockito.verify(client, Mockito.times(1)).getEmrClient(Mockito.any(EmrClusterConfig.class));
  }



}
