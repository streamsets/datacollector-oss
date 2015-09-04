/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
import java.util.Properties;

public class BootstrapSparkFunction<T1, T2> implements VoidFunction<Iterator<Tuple2<T1, T2>>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapSparkFunction.class);
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private volatile boolean initialized = false;
  private ClusterFunction clusterFunction;
  private Properties properties;
  private int batchSize;

  public BootstrapSparkFunction() {
  }

  private synchronized void initialize() throws Exception {
    if (initialized) {
      return;
    }
    clusterFunction = (ClusterFunction)BootstrapCluster.getClusterFunction(TaskContext.get().partitionId());
    properties = BootstrapCluster.getProperties();
    batchSize = Integer.parseInt(properties.getProperty("production.maxBatchSize", "1000").trim());
    initialized = true;
  }

  @Override
  public void call(Iterator<Tuple2<T1, T2>> tupleIterator) throws Exception {
    initialize();
    List<Map.Entry> batch = new ArrayList<>();
    while (tupleIterator.hasNext()) {
      Tuple2<T1, T2> tuple = tupleIterator.next();
      if (IS_TRACE_ENABLED) {
        LOG.trace("Got message: 1: {}, 2: {}", toString((byte[]) tuple._1), toString((byte[]) tuple._2));
      }
      if (batch.size() == batchSize) {
        clusterFunction.invoke(batch);
        batch = new ArrayList<>();
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
