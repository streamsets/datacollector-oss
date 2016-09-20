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
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;
import com.streamsets.pipeline.impl.ClusterFunction;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class ClusterFunctionImpl implements ClusterFunction  {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterFunctionImpl.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static volatile EmbeddedSDCPool sdcPool;
  private static volatile boolean initialized = false;
  private static volatile String errorStackTrace;

  private static synchronized void initialize(Properties properties, Integer id, String rootDataDir) throws Exception {
    if (initialized) {
      return;
    }
    File dataDir = new File(System.getProperty("user.dir"), "data");
    FileUtils.copyDirectory(new File(rootDataDir), dataDir);
    System.setProperty("sdc.data.dir", dataDir.getAbsolutePath());
    // must occur before creating the EmbeddedSDCPool as
    // the hdfs target validation evaluates the sdc:id EL
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(6);
    numberFormat.setGroupingUsed(false);
    final String sdcId = numberFormat.format(id);
    Utils.setSdcIdCallable(new Callable<String>() {
      @Override
      public String call() {
        return sdcId;
      }
    });
    sdcPool = new EmbeddedSDCPool(properties);
    initialized = true;
  }

  public static ClusterFunction create(Properties properties, Integer id, String rootDataDir) throws Exception {
    initialize(Utils.checkNotNull(properties, "Properties"), id, rootDataDir);
    return new ClusterFunctionImpl();
  }

  @Override
  public void invoke(List<Map.Entry> batch) throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
    }
    if (errorStackTrace != null) {
      LOG.info("Not proceeding as error in previous run");
      throw new RuntimeException(errorStackTrace);
    }
    try {
      EmbeddedSDC embeddedSDC = sdcPool.checkout();
      embeddedSDC.getSource().put(batch);
    } catch (Exception | Error e) {
      // Get the stacktrace as string as the spark driver wont have the jars
      // required to deserialize the classes from the exception cause
      errorStackTrace = getErrorStackTrace(e);
      throw new RuntimeException(errorStackTrace);
    }
  }

  private String getErrorStackTrace(Throwable e) {
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
    EmbeddedSDC embeddedSDC = sdcPool.checkout();
    // shutdown is special call which causes the pipeline origin to
    // return null resulting in the pipeline finishing normally
    embeddedSDC.getSource().shutdown();
    sdcPool.checkin(embeddedSDC);
  }

  public static long getRecordsProducedJVMWide() {
    long result = 0;
    if (sdcPool == null) {
      throw new RuntimeException("Embedded SDC pool is not initialized");
    }
    for (EmbeddedSDC sdc : sdcPool.getInstances()) {
      result += sdc.getSource().getRecordsProduced();
    }
    return result;
  }
}
