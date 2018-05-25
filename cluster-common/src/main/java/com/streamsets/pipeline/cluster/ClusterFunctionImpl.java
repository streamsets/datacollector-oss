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
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.BootstrapCluster;
import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.impl.ClusterFunction;
import com.streamsets.pipeline.spark.RecordCloner;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ClusterFunctionImpl implements ClusterFunction  {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterFunctionImpl.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static volatile EmbeddedSDCPool sdcPool;
  private static volatile boolean initialized = false;
  private static volatile String errorStackTrace;
  private volatile Object offset;

  private static final String GET_BATCH = "getBatch";
  private static final String SET_ERRORS = "setErrors";
  private static final String CONTINUE_PROCESSING = "continueProcessing";

  private static synchronized void initialize(Properties properties, String id, String rootDataDir) throws Exception {
    if (initialized) {
      return;
    }
    File dataDir = new File(System.getProperty("user.dir"), "data");
    FileUtils.copyDirectory(new File(rootDataDir), dataDir);
    System.setProperty("sdc.data.dir", dataDir.getAbsolutePath());
    // must occur before creating the EmbeddedSDCPool as
    // the hdfs target validation evaluates the sdc:id EL
    Utils.setSdcIdCallable(() -> id);
    sdcPool = EmbeddedSDCPool.createPool(properties);
    initialized = true;
  }

  public void setSparkProcessorCount(int count) {
    sdcPool.setSparkProcessorCount(count);
  }

  public static synchronized boolean isInitialized() {
    return initialized;
  }

  public static ClusterFunction create(Properties properties, String id, String rootDataDir) throws Exception {
    initialize(Utils.checkNotNull(properties, "Properties"), id, rootDataDir);
    ClusterFunctionImpl fn = new ClusterFunctionImpl();
    fn.setSparkProcessorCount(BootstrapCluster.getSparkProcessorLibraryNames().size());
    return fn;
  }

  @Override
  public Iterator startBatch(List<Map.Entry> batch) throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
    }
    if (errorStackTrace != null) {
      LOG.info("Not proceeding as error in previous run");
      throw new RuntimeException(errorStackTrace);
    }
    EmbeddedSDC sdc = null;
    try {
      sdc = sdcPool.getNotStartedSDC();
      ClusterSource source = sdc.getSource();
      offset = source.put(batch);
      return getNextBatch(0, sdc);
    } catch (Exception | Error e) {
      // Get the stacktrace as string as the spark driver wont have the jars
      // required to deserialize the classes from the exception cause
      errorStackTrace = getErrorStackTrace(e);
      throw new RuntimeException(errorStackTrace);
    } finally {
      if (sdc != null) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Checking SDC: " + sdc + " back in after starting batch");
        }
        try {
          sdcPool.checkInAfterReadingBatch(0, sdc);
        } catch (Exception ex) {
          errorStackTrace = getErrorStackTrace(ex);
          throw new RuntimeException(errorStackTrace);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static Iterator<Object> getNextBatch(int id, EmbeddedSDC sdc) {
    return Optional.ofNullable(sdc.getSparkProcessorAt(id)).flatMap(t -> {
      try {
        Object processor = t.getClass().getMethod("get").invoke(t);
        final Method getBatch = processor.getClass().getDeclaredMethod(GET_BATCH);
        List<Object> cloned = new ArrayList<>();
        Iterator<Object> batch = (Iterator<Object>) getBatch.invoke(processor);
        batch.forEachRemaining(record -> cloned.add(RecordCloner.clone(record)));
        return Optional.of(cloned.iterator());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }).orElse(Collections.emptyIterator());
  }

  @Override
  public void writeErrorRecords(Iterator errors, int id) throws Exception {
    EmbeddedSDC sdc = sdcPool.getSDCBatchRead(id);
    try {
      Optional.ofNullable(sdc.getSparkProcessorAt(id)).ifPresent(t -> {
        try {
          Object processor = t.getClass().getMethod("get").invoke(t);
          final Method setErrors = processor.getClass().getDeclaredMethod(SET_ERRORS, Iterator.class);
          setErrors.invoke(processor, errors);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    } finally {
      if (sdc != null) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Checking SDC: " + sdc +" back in after writing errors for id " + id);
        }
        // No exception since this does not proceed to next batch, so no waitForCommit call.
        sdcPool.checkInAfterReadingBatch(id, sdc);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Object> forwardTransformedBatch(Iterator batch, int id) throws Exception {
    EmbeddedSDC sdc = sdcPool.getSDCBatchRead(id);
    try {
      return doForward(batch, id, sdc);
    } finally {
      if (sdc != null) {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Checking SDC: " + sdc +" back in after writing batch for id " + id + 1);
        }
        try {
          sdcPool.checkInAfterReadingBatch(id + 1, sdc);
        } catch (Exception ex) {
          errorStackTrace = getErrorStackTrace(ex);
          throw new RuntimeException(errorStackTrace);
        }
      }
    }
  }

  public static Iterator<Object> doForward(Iterator batch, int id, EmbeddedSDC sdc) {
    return Optional.ofNullable(sdc.getSparkProcessorAt(id)).map(t -> {
      try {
        Object processor = t.getClass().getMethod("get").invoke(t);
        final Method continueProcessing = processor.getClass().getDeclaredMethod(CONTINUE_PROCESSING, Iterator.class);
        continueProcessing.invoke(processor, batch);
        return getNextBatch(id + 1, sdc);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }).orElse(Collections.emptyIterator());
  }

  private static String getErrorStackTrace(Throwable e) {
    StringWriter errorWriter = new StringWriter();
    PrintWriter pw = new PrintWriter(errorWriter);
    pw.println();
    e.printStackTrace(pw);
    pw.flush();
    return errorWriter.toString();
  }

  @Override
  public void shutdown() throws Exception {
    LOG.info("Shutdown");
    Utils.checkState(initialized, "Not initialized");
    sdcPool.shutdown();
  }
}
