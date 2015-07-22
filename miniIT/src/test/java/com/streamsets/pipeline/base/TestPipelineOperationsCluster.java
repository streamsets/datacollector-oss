/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.base;

import com.streamsets.datacollector.MiniSDC;
import com.streamsets.pipeline.util.ClusterUtil;
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
public abstract class TestPipelineOperationsCluster extends TestPipelineOperationsBase {

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
