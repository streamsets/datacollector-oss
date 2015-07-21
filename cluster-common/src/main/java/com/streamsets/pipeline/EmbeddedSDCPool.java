/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ClusterSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import com.streamsets.pipeline.configurablestage.DSource;

/**
 * Creates a pool of embedded SDC's to be used within a single executor (JVM)
 *
 */
public class EmbeddedSDCPool {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDCPool.class);
  private final ConcurrentLinkedDeque<EmbeddedSDC> concurrentQueue = new ConcurrentLinkedDeque<EmbeddedSDC>();
  private final List<EmbeddedSDC> totalInstances = new CopyOnWriteArrayList<>();
  private final Properties properties;
  private final String pipelineJson;
  private final boolean infiniteSDCPool;
  /**
   * Create a pool. For now, there is only instance being created
   * @param properties the properties file
   * @param pipelineJson the pipeline configuration
   * @throws Exception
   */
  public EmbeddedSDCPool(Properties properties, String pipelineJson) throws Exception {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson,  "Pipeline JSON");
    infiniteSDCPool = Boolean.valueOf(properties.getProperty("sdc.pool.size.infinite", "false"));
    addToQueues(createEmbeddedSDC());
  }

  /**
   * Creates an instance of SDC and adds to pool
   * @return EmbeddedSDC
   * @throws Exception
   */
  protected EmbeddedSDC createEmbeddedSDC() throws Exception {
    final EmbeddedSDC embeddedSDC = new EmbeddedSDC();
    Object source;
    source = BootstrapCluster.startPipeline(new Runnable() { // post-batch runnable
        @Override
        public void run() {
          if (!embeddedSDC.inErrorState()) {
            LOG.debug("Returning SDC instance {} back to queue", embeddedSDC.getInstanceId());
            returnEmbeddedSDC(embeddedSDC);
          } else {
            LOG.info("SDC is in error state, not returning to pool");
          }
        }
      });

    if (source instanceof DSource) {
      long startTime = System.currentTimeMillis();
      long endTime = startTime;
      long diff = endTime - startTime;
      Source actualSource = ((DSource) source).getSource();
      while (actualSource == null && diff < 60000) {
        Thread.sleep(100);
        actualSource = ((DSource) source).getSource();
        endTime = System.currentTimeMillis();
        diff = endTime - startTime;
      }
      if (actualSource == null) {
        throw new IllegalStateException("Actual source is null, pipeline may not have been initialized");
      }
      source = actualSource;
    }

    if (!(source instanceof ClusterSource)) {
      throw new IllegalArgumentException("Source is not of type SparkStreamingSource: " + source.getClass().getName());
    }
    embeddedSDC.setSource((ClusterSource) source);
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
      if (infiniteSDCPool) {
        LOG.warn("Creating new SDC as no SDC found in queue, This should be called only during testing");
        embeddedSDC = createEmbeddedSDC();
        addToQueues(embeddedSDC);
        embeddedSDC = concurrentQueue.poll();
      } else {
        // wait for a minute for sdc to be returned back
        embeddedSDC = waitForSDC(60000);
      }
    } else {
      embeddedSDC = concurrentQueue.poll();
    }
    if (embeddedSDC == null) {
      throw new IllegalStateException("Cannot find SDC, this should never happen");
    }
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
    if (!concurrentQueue.contains(embeddedSDC)) {
      concurrentQueue.offer(embeddedSDC);
    }
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

  @VisibleForTesting
  EmbeddedSDC waitForSDC(long timeout) throws InterruptedException {
    EmbeddedSDC embeddedSDC = null;
    if (timeout < 0) throw new IllegalArgumentException("Timeout shouldn't be less than zero");
    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    long diff = endTime - startTime;
    int counter = 1000;
    while (diff < timeout) {
      embeddedSDC = concurrentQueue.poll();
      if (embeddedSDC != null) {
        break;
      }
      Thread.sleep(50);
      endTime = System.currentTimeMillis();
      diff = endTime - startTime;
      if (diff > counter) {
        LOG.warn("Have been waiting for sdc for " + diff + "ms");
        // as we want to print messages every second
        counter = counter + 1000;
      }
    }
    return embeddedSDC;
  }

}
