/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;
import com.streamsets.pipeline.impl.ClusterFunction;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class ClusterFunctionImpl implements ClusterFunction  {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterFunctionImpl.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static volatile EmbeddedSDCPool sdcPool;
  private static volatile boolean initialized = false;

  private static synchronized void initialize(Properties properties, Integer id) throws Exception {
    if (initialized) {
      return;
    }
    initialized = true;
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
  }

  public static ClusterFunction create(Properties properties, Integer id) throws Exception {
    initialize(Utils.checkNotNull(properties, "Properties"), id);
    return new ClusterFunctionImpl();
  }

  @Override
  public void invoke(Object... args) throws Exception {
    if (args == null || args.length != 1) {
      throw new IllegalArgumentException("Expected one argument, got: " + Arrays.toString(args));
    }
    if (!(args[0] instanceof List)) {
      throw new IllegalArgumentException("Expected argument to be list, got: " + args[0].getClass().getName());
    }
    List<Map.Entry> batch = (List)args[0];
    if (IS_TRACE_ENABLED) {
      LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
    }
    EmbeddedSDC embeddedSDC = sdcPool.checkout();
    embeddedSDC.getSource().put(batch);
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
