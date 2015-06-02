/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;
import com.streamsets.pipeline.Pair;
import com.streamsets.pipeline.api.impl.Utils;


import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;


/**
 * This function is serialized and pushed over the write to all executors
 */
public class SparkExecutorFunction<T1, T2> implements VoidFunction<Iterator<Tuple2<T1, T2>>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutorFunction.class);
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static volatile EmbeddedSDCPool sdcPool;
  private static final Object staticLock = new Object();
  private static volatile boolean initialized = false;
  private Properties properties;
  private String pipelineJson;

  public SparkExecutorFunction(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson,  "Pipeline JSON");
  }

  private void initialize() throws Exception {
    synchronized (staticLock) {
      if (!initialized) {
        // must occur before creating the EmbeddedSDCPool as
        // the hdfs target validation evaluates the sdc:id EL
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(6);
        numberFormat.setGroupingUsed(false);
        final String sdcId = numberFormat.format(TaskContext.get().partitionId());
        Utils.setSdcIdCallable(new Callable<String>() {
          @Override
          public String call() {
            return sdcId;
          }
        });
        sdcPool = new EmbeddedSDCPool(properties, pipelineJson);
        initialized = true;
      }
    }
  }

  @Override
  public void call(Iterator<Tuple2<T1, T2>> tupleIterator) throws Exception {
    LOG.debug("In executor function " + " " + Thread.currentThread().getName());
    initialize();
    EmbeddedSDC embeddedSDC = sdcPool.getEmbeddedSDC();
    List<Pair> batch = new ArrayList<>();
    boolean hdfs = embeddedSDC.getSource().getName().equals("hdfs");

    while (tupleIterator.hasNext()) {
      Tuple2<T1, T2> tuple = tupleIterator.next();
      if (hdfs) {
        // For now assuming this is text
        batch.add(new Pair(Long.valueOf(tuple._1().toString()), tuple._2().toString()));
        LOG.info("Got message: 1: {}, 2: {}", tuple._1(), tuple._2());
      } else {
        if (IS_TRACE_ENABLED) {
          LOG.trace("Got message: 1: {}, 2: {}", toString((byte[])tuple._1), toString((byte[])tuple._2));
        }
        batch.add(new Pair(tuple._1(), tuple._2()));
      }
    }
    embeddedSDC.getSource().put(batch);
  }

  private static String toString(byte[] buf) {
    if (buf == null) {
      return "null";
    }
    char[] chars = new char[2 * buf.length];
    for (int i = 0; i < buf.length; ++i) {
      chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
      chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
    }
    return new String(chars);
  }

  public static <T1, T2> void
    execute(Properties properties, String pipelineJson, Iterator<Tuple2<T1, T2>> tupleIterator)
      throws Exception {
    SparkExecutorFunction<T1, T2> function = new SparkExecutorFunction<T1, T2>(properties, pipelineJson);
    function.call(tupleIterator);
  }

  public static long getRecordsProducedJVMWide() {
    long result = 0;
    if (sdcPool == null) {
      throw new RuntimeException("Embedded SDC pool is not initialized");
    }
    for (EmbeddedSDC sdc : sdcPool.getTotalInstances()) {
      result += sdc.getSource().getRecordsProduced();
    }
    return result;
  }

}
