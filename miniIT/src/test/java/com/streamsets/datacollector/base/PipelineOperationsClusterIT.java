/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.base;

import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.util.ClusterUtil;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * The pipeline and data provider for this test case should make sure that the pipeline runs continuously and the origin
 *  in the pipeline keeps producing records until stopped.
 *
 * Origin has to be Kafka as of now. Make sure that there is continuous supply of data for the pipeline to keep running.
 * For example to test kafka Origin, a background thread could keep writing to the kafka topic from which the
 * kafka origin reads.
 *
 */
public abstract class PipelineOperationsClusterIT extends PipelineOperationsBaseIT {

  protected static URI serverURI;
  protected static MiniSDC miniSDC;

  public static void beforeClass(String pipelineJson, String testName) throws Exception {
    ClusterUtil.setupCluster(testName, pipelineJson, new YarnConfiguration());
    serverURI = ClusterUtil.getServerURI();
    miniSDC = ClusterUtil.getMiniSDC();
  }

  public static void afterClass(String testName) throws Exception {
    ClusterUtil.tearDownCluster(testName);
  }

  protected URI getServerURI() {
    return serverURI;
  }

  protected List<URI> getWorkerURI() throws URISyntaxException {
    return miniSDC.getListOfSlaveSDCURI();
  }

  protected  boolean clusterModeTest() {
    return true;
  }

}
