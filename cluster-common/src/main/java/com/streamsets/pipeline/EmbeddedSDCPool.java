/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.ClusterSource;
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
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private final ConcurrentLinkedDeque<EmbeddedSDC> instanceQueue = new ConcurrentLinkedDeque<>();
  private final List<EmbeddedSDC> instances = new CopyOnWriteArrayList<>();
  private final Properties properties;
  private boolean infinitePoolSize;
  private volatile boolean open;

  /**
   * Create a pool. For now, there is only instance being created
   * @param properties the properties file
   * @throws Exception
   */
  public EmbeddedSDCPool(Properties properties) throws Exception {
    this.open = true;
    this.properties = properties;
    infinitePoolSize = Boolean.valueOf(properties.getProperty("sdc.pool.size.infinite", "false"));
    addToQueues(create());
  }

  /**
   * Creates an instance of SDC and adds to pool
   * @return EmbeddedSDC
   * @throws Exception
   */
  protected EmbeddedSDC create() throws Exception {
    Utils.checkState(open, "Not open");
    final EmbeddedSDC embeddedSDC = new EmbeddedSDC();
    Object source;
    source = BootstrapCluster.startPipeline(new Runnable() { // post-batch runnable
      @Override
      public void run() {
        LOG.debug(
            "Returning SDC instance: '{}' in state: '{}' back to queue",
            embeddedSDC.getInstanceId(),
            !embeddedSDC.inErrorState() ? "SUCCESS" : "ERROR"
        );
        checkin(embeddedSDC);
      }
    });
    if (source instanceof DSource) {
      long startTime = System.currentTimeMillis();
      long endTime = startTime;
      long diff = 0;
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
        throw new IllegalArgumentException("Source is not of type ClusterSource: " + source.getClass().getName());
    }
    embeddedSDC.setSource((ClusterSource) source);
    return embeddedSDC;
  }

  private void addToQueues(EmbeddedSDC embeddedSDC) throws Exception {
    Utils.checkState(open, "Not open");
    instances.add(embeddedSDC);
    instanceQueue.add(embeddedSDC);
    if (IS_TRACE_ENABLED) {
      LOG.trace("After adding, size of queue is " + instanceQueue.size());
    }
  }

  /**
   * Get an instance of embedded SDC
   * @return
   * @throws Exception
   */
  public EmbeddedSDC checkout() throws Exception {
    Utils.checkState(open, "Not open");
    if (IS_TRACE_ENABLED) {
      LOG.trace("Before polling, size of queue is " + instanceQueue.size());
    }
    EmbeddedSDC embeddedSDC;
    if (instanceQueue.size() == 0) {
      if (infinitePoolSize) {
        LOG.warn("Creating new SDC as no SDC found in queue, This should be called only during testing");
        embeddedSDC = create();
        addToQueues(embeddedSDC);
        embeddedSDC = instanceQueue.poll();
      } else {
        // wait for a minute for sdc to be returned back
        embeddedSDC = waitForSDC(60000);
      }
    } else {
      embeddedSDC = instanceQueue.poll();
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
  public synchronized int size() {
    Utils.checkState(open, "Not open");
    return instanceQueue.size();
  }

  /**
   * Return the embedded SDC back. This function is called once the batch of
   * RDD's is processed
   */
  public void checkin(EmbeddedSDC embeddedSDC) {
    // don't check open since we want the instance returned per the
    // condition in destroy blocking for all instances to be returned
    if (!instanceQueue.contains(embeddedSDC)) {
      instanceQueue.offer(embeddedSDC);
    }
    if (IS_TRACE_ENABLED) {
      LOG.trace("After returning an SDC, size of queue is " + instanceQueue.size());
    }
  }

  /**
   * Get total instances of SDC (used and unused)
   * @return the list of SDC
   */
  public List<EmbeddedSDC> getInstances() {
    Utils.checkState(open, "Not open");
    return instances;
  }

  @VisibleForTesting
  EmbeddedSDC waitForSDC(long timeout) throws InterruptedException {
    EmbeddedSDC embeddedSDC = null;
    if (timeout < 0) throw new IllegalArgumentException("Timeout shouldn't be less than zero");
    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    long diff = 0;
    int counter = 1000;
    while (diff < timeout) {
      Utils.checkState(open, "Not open");
      embeddedSDC = instanceQueue.poll();
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
