/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hadoop;

import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class HadoopMapFunction {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMapFunction.class);
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

  public void call(List<Map.Entry> batch) throws Exception {
    if (IS_TRACE_ENABLED) {
      LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
    }
    EmbeddedSDC embeddedSDC = sdcPool.checkout();
    embeddedSDC.getSource().put(batch);
  }

  public static void shutdown() throws Exception {
    LOG.info("Shutdown");
    Utils.checkState(initialized, "Not initialized");
    EmbeddedSDC embeddedSDC = sdcPool.checkout();
    // shutdown is special call which causes the pipeline origin to
    // return null resulting in the pipeline finishing normally
    embeddedSDC.getSource().shutdown();
    sdcPool.checkin(embeddedSDC);
  }

  public static void execute(Properties properties, Integer id, List<Map.Entry> batch)
    throws Exception {
    initialize(Utils.checkNotNull(properties, "Properties"), id);
    HadoopMapFunction function = new HadoopMapFunction();
    function.call(batch);
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
