/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import com.streamsets.pipeline.BootstrapSpark;
import com.streamsets.pipeline.configurablestage.DSource;

/**
 * Creates a pool of embedded SDC's to be used within a single executor (JVM)
 *
 */
public class EmbeddedSDCPool {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDCPool.class);
  private ConcurrentLinkedDeque<EmbeddedSDC> concurrentQueue = new ConcurrentLinkedDeque<EmbeddedSDC>();
  private final List<EmbeddedSDC> totalInstances = new CopyOnWriteArrayList<>();
  private Properties properties;
  private String pipelineJson;

  /**
   * Create a pool. For now, there is only instance being created
   * @param properties the properties file
   * @param pipelineJson the pipeline configuration
   * @throws Exception
   */
  public EmbeddedSDCPool(Properties properties, String pipelineJson) throws Exception {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson,  "Pipeline JSON");
    addToQueues(createEmbeddedSDC());
  }

  /**
   * Creates an instance of SDC and adds to pool
   * @return EmbeddedSDC
   * @throws Exception
   */
  protected EmbeddedSDC createEmbeddedSDC() throws Exception {
    final EmbeddedSDC embeddedSDC = new EmbeddedSDC();
    Object source = BootstrapSpark.createPipeline(properties, pipelineJson, new Runnable() { // post-batch runnable
        @Override
        public void run() {
          LOG.debug("Returning SDC instance {} back to queue", embeddedSDC.getInstanceId());
          returnEmbeddedSDC(embeddedSDC);
        }
      });

    if (source instanceof DSource) {
      source = ((DSource) source).getSource();
    }

    if (!(source instanceof SparkStreamingSource)) {
      throw new IllegalArgumentException("Source is not of type SparkStreamingSource: " + source.getClass().getName());
    }
    embeddedSDC.setSource((SparkStreamingSource) source);
    return embeddedSDC;
  }

  private void addToQueues(EmbeddedSDC embeddedSDC) throws Exception {
    totalInstances.add(embeddedSDC);
    concurrentQueue.add(embeddedSDC);
    LOG.debug("After adding, size of queue is " + concurrentQueue.size());
  }

  /**
   * Get an instance of embedded SDC
   * @return
   * @throws Exception
   */
  public EmbeddedSDC getEmbeddedSDC() throws Exception {
    LOG.debug("Before polling, size of queue is " + concurrentQueue.size());
    EmbeddedSDC embeddedSDC;
    if (concurrentQueue.size() == 0) {
      LOG.debug("No SDC found in queue, creating new one");
      embeddedSDC = createEmbeddedSDC();
      addToQueues(embeddedSDC);
    }
    embeddedSDC = concurrentQueue.poll();
    return embeddedSDC;
  }

  /**
   * Return size of pool
   * @return
   */
  public int size() {
    return concurrentQueue.size();
  }

  /**
   * Return the embedded SDC back. This function is called once the batch of
   * RDD's is processed
   * @param embeddedSDC
   */
  public void returnEmbeddedSDC(EmbeddedSDC embeddedSDC) {
    concurrentQueue.offer(embeddedSDC);
    LOG.debug("After returning an SDC, size of queue is " + concurrentQueue.size());
  }

  public void destoryEmbeddedSDC() {
    //
  }

  /**
   * Get total instances of SDC (used and unused)
   * @return the list of SDC
   */
  public List<EmbeddedSDC> getTotalInstances() {
    return totalInstances;
  }
}
