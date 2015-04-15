/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.BatchListener;

public class EmbeddedSDCPool {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDCPool.class);
  private ConcurrentLinkedDeque<EmbeddedSDC> concurrentQueue = new ConcurrentLinkedDeque<EmbeddedSDC>();
  private final List<EmbeddedSDC> totalInstances = new CopyOnWriteArrayList<>();

  public EmbeddedSDCPool(int min, EmbeddedSDCConf embeddedSDCConf) throws Exception {
    for (int i = 0; i < min; i++) {
      createEmbeddedSDC(embeddedSDCConf);
    }
  }

  private EmbeddedSDC createEmbeddedSDC(EmbeddedSDCConf embeddedSDCConf) throws Exception {
    final EmbeddedSDC embeddedSDC = new EmbeddedSDC(embeddedSDCConf);
    Pipeline pipeline = embeddedSDC.init();
    pipeline.getRunner().registerListener(new BatchListener() {
      @Override
      public void postBatch() {
        LOG.debug("Returning SDC instance back to queue");
        returnEmbeddedSDC(embeddedSDC);
      }

      @Override
      public void preBatch() {
        //
      }
    });
    totalInstances.add(embeddedSDC);
    concurrentQueue.add(embeddedSDC);
    LOG.debug("After adding, size of queue is " + concurrentQueue.size());
    return embeddedSDC;
  }

  public EmbeddedSDC getEmbeddedSDC(EmbeddedSDCConf embeddedSDCConf) throws Exception {
    LOG.debug("Before polling, size of queue is " + concurrentQueue.size());
    EmbeddedSDC embeddedSDC = concurrentQueue.poll();
    if (embeddedSDC == null) {
      LOG.debug("No SDC found in queue, creating new one");
      embeddedSDC = createEmbeddedSDC(embeddedSDCConf);
    }
    return embeddedSDC;
  }

  public int size() {
    return concurrentQueue.size();
  }

  public void returnEmbeddedSDC(EmbeddedSDC embeddedSDC) {
    concurrentQueue.offer(embeddedSDC);
    LOG.debug("After returning an SDC, size of queue is " + concurrentQueue.size());
  }

  public void destoryEmbeddedSDC() {
    //
  }

  public List<EmbeddedSDC> getTotalInstances() {
    return totalInstances;
  }
}
