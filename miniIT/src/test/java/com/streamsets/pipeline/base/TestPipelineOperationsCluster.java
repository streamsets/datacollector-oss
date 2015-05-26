/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.base;

import com.streamsets.pipeline.MiniSDC;
import com.streamsets.pipeline.util.ClusterUtil;

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

  private static final String TEST_NAME = "PipelineOperationsOnCluster";

  public static void beforeClass(String pipelineJson) throws Exception {
    ClusterUtil.setupCluster(TEST_NAME, pipelineJson);
    serverURI = ClusterUtil.getServerURI();
    miniSDC = ClusterUtil.getMiniSDC();
  }

  public static void afterClass() throws Exception {
    ClusterUtil.tearDownCluster(TEST_NAME);
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
