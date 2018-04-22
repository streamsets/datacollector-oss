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
package com.streamsets.pipeline;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Creates a pool of embedded SDC's to be used within a single executor (JVM)
 *
 */
public class EmbeddedSDCPool {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSDCPool.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private final Properties properties;
  private boolean infinitePoolSize;
  private volatile boolean open;
  private static final ReentrantLock lock = new ReentrantLock();
  private static EmbeddedSDCPool instance;

  /**
   * notStarted => Batch has not started yet, so no data read from kafka
   * notStarted -> waitingForBatchToBeRead (0)
   *
   * waitingForBatchToBeRead (id) =>
   *   Embedded SDC whose spark processor at location (i) whose data has been read into the pipeline,
   *   but not read from the pipeline to the transformer.
   * waitingForBatchToBeRead (id) -> Transformer #id.
   *
   * batchRead (id) =>
   *   SDC whose batch has been read into transformer at id, but processed/error data has not arrived.
   * Transformer #id -> batchRead (id)
   *
   * errorsWritten (id) =>
   *   SDC whose batch has been processed by transformer and errors have been written to the processor at (id)
   *
   * batchRead (id) -> errorsWritten (id)
   *   Once a transformer is completed, it will pick up an Embedded SDC whose batch has been read at (id),
   *   and then write errors to it. Once errors are written, it will check the SDC into errorsWritten at (id)
   *
   * errorsWritten (id) -> waitingForBatchToBeRead (id + 1) or notStarted (if id + 1 == sparkProcessorCount)
   *   Once errors are written, it is checked out and the mapping function will now write out the results from
   *   the transformer. If there are more processors, the sdc is added to  waitingForBatchToBeRead at (id + 1), else
   *   we wait for batch completion, and then add the sdc to notStarted.
   *
   */
  private Set<EmbeddedSDC> notStarted = new HashSet<>();
  private Map<Integer, Deque<EmbeddedSDC>> batchRead = new HashMap<>();
  private Set<EmbeddedSDC> used = new HashSet<>();
  private int sparkProcessorCount;

  /**
   * Create a pool. For now, there is only instance being created
   * @param properties the properties file
   * @throws Exception
   */
  protected EmbeddedSDCPool(Properties properties) throws Exception {
    this.open = true;
    this.properties = properties;
    infinitePoolSize = Boolean.valueOf(properties.getProperty("sdc.pool.size.infinite", "true"));
  }


  public static EmbeddedSDCPool createPool(Properties properties) throws Exception {
    lock.lock();
    try {
      Preconditions.checkArgument(instance == null, "EmbeddedSDCPool has already been created.");
      instance = new EmbeddedSDCPool(properties);
      return instance;
    } finally {
      lock.unlock();
    }
  }

  public static EmbeddedSDCPool getPool() {
    lock.lock();
    try {
      Preconditions.checkArgument(instance != null, "EmbeddedSDCPool has not been initialized yet");
      return instance;
    } finally {
      lock.unlock();
    }
  }

  public void setSparkProcessorCount(int count) {
    this.sparkProcessorCount = count;
  }

  /**
   * Creates an instance of SDC and adds to pool
   * @return EmbeddedSDC
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  protected EmbeddedSDC create() throws Exception {
    Utils.checkState(open, "Not open");
    final EmbeddedSDC embeddedSDC = new EmbeddedSDC();
    Object source;
    // post-batch runnable
    Object pipelineStartResult = BootstrapCluster.startPipeline(() -> LOG.debug("Batch completed"));
    source = pipelineStartResult.getClass().getDeclaredField("source").get(pipelineStartResult);
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
    embeddedSDC.setSparkProcessors(
        (List<Object>)pipelineStartResult.getClass().getDeclaredField("sparkProcessors").get(pipelineStartResult));
    return embeddedSDC;
  }

  public synchronized EmbeddedSDC getNotStartedSDC() throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Getting SDC whose pipeline has not started.");
    }
    EmbeddedSDC sdc;
    Iterator<EmbeddedSDC> notStartedIter = notStarted.iterator();
    if (notStartedIter.hasNext()) {
      sdc = notStartedIter.next();
      notStartedIter.remove();
    } else {
      sdc = create();
    }
    used.add(sdc);
    return sdc;
  }

  public synchronized EmbeddedSDC getSDCBatchRead(int id) throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Getting SDC whose batch been read for id: " + id);
    }
    // This may require a fast-forward. Due to shuffle, we may end calling this one without actually having an SDC
    // at this state, so we must create one and fast-forward it to this one by sending an empty batch
    return getOrCreate(id, batchRead);
  }

  public synchronized void checkInAfterReadingBatch(int id, EmbeddedSDC sdc) throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("Checking SDC in after batch written for id: " + id);
    }
    if  (id == sparkProcessorCount) {
      sdc.getSource().completeBatch();
      notStarted.add(sdc);
      used.remove(sdc);
    } else {
      checkInAtId(id, sdc, batchRead);
    }
  }

  /**
   * What is this?!
   * Spark could end up scheduling transformers to run on machines that may actually not have received the data, due to:
   * - Non-local reads: Spark Locality is time-bound, so if local executor where the data is located is not available,
   *     Spark will tell another one to pick up the task.
   * - Shuffle in the transformer: After a shuffle, it is possible the next task passing the data to the next batch could
   *     end up on a different executor.
   *
   * In both these cases, it is possible that there is no SDC at that id. So we create a new SDC and fast-forward to
   * that id. Since a read can never happen from a fast-forwarded id, we read the fake batch we passed till there, and
   * then return the sdc so it is ready for writes/errors.
   *
   */
  private EmbeddedSDC fastForward(int id) throws Exception {
    if (id == sparkProcessorCount) {
      return null;
    }
    LOG.info("No SDC was found at ID: " + id + ". Fast-forwarding..");
    EmbeddedSDC sdc;
    // If there are not SDCs that are just idling, create a new one, else return one from the not started pool.
    if (notStarted.isEmpty()) {
      sdc = create();
    } else {
      sdc = getNotStartedSDC();
    }

    Class<?> clusterFunctionClass = Class.forName("com.streamsets.pipeline.cluster.ClusterFunctionImpl");
    Method getBatch = clusterFunctionClass.getMethod("getNextBatch", int.class, EmbeddedSDC.class);
    Method forward =
        clusterFunctionClass.getMethod("doForward", Iterator.class, int.class, EmbeddedSDC.class);
    sdc.getSource().put(Collections.emptyList());
    getBatch.invoke(null, 0, sdc);
    for (int i = 0; i <= id - 1; i++) {
      forward.invoke(null, Collections.emptyIterator(), i, sdc);
    }
    return sdc;
  }

  private synchronized void checkInAtId(
      int id,
      EmbeddedSDC sdc,
      Map<Integer, Deque<EmbeddedSDC>> sdcMap
  ) throws Exception {
    Deque<EmbeddedSDC> sdcList = sdcMap.computeIfAbsent(id, i -> new ArrayDeque<>());
    sdcList.addLast(sdc);
    used.remove(sdc);
  }

  private EmbeddedSDC getOrCreate(int id, Map<Integer, Deque<EmbeddedSDC>> sdcMap) throws Exception {
    Deque<EmbeddedSDC> deque = sdcMap.computeIfAbsent(id, key -> new ArrayDeque<>());
    EmbeddedSDC sdc;
    if (!deque.isEmpty()) {
      sdc = deque.removeFirst();
    } else {
      sdc = fastForward(id);
    }
    used.add(sdc);
    return sdc;
  }

  public synchronized void shutdown() {
    this.open = false;
    used.forEach(sdc -> sdc.getSource().shutdown());
    shutdownAllInMap(batchRead);
    notStarted.forEach(sdc -> sdc.getSource().shutdown());
  }

  private void shutdownAllInMap(Map<Integer, Deque<EmbeddedSDC>> sdcMap) {
    sdcMap.values().forEach(sdcList -> sdcList.forEach(sdc -> sdc.getSource().shutdown()));
  }

}
