/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.BootstrapCluster;

import com.streamsets.pipeline.impl.ClusterFunction;
import com.streamsets.pipeline.impl.Pair;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.VoidFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BootstrapSparkFunction<T1, T2> implements VoidFunction<Iterator<Tuple2<T1, T2>>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapSparkFunction.class);
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private volatile boolean initialized = false;
  private ClusterFunction clusterFunction;

  public BootstrapSparkFunction() {
  }

  private synchronized void initialize() throws Exception {
    if (initialized) {
      return;
    }
    clusterFunction = (ClusterFunction)BootstrapCluster.getClusterFunction(TaskContext.get().partitionId());
    initialized = true;
  }

  @Override
  public void call(Iterator<Tuple2<T1, T2>> tupleIterator) throws Exception {
    initialize();
    List<Map.Entry> batch = new ArrayList<>();
    while (tupleIterator.hasNext()) {
      Tuple2<T1, T2> tuple = tupleIterator.next();
      if (IS_TRACE_ENABLED) {
        LOG.trace("Got message: 1: {}, 2: {}", toString((byte[])tuple._1), toString((byte[])tuple._2));
      }
      batch.add(new Pair(tuple._1(), tuple._2()));
    }
    clusterFunction.invoke(batch);
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
}
