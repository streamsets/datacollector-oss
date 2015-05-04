/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.streamsets.pipeline.api.impl.Utils;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.stage.origin.kafka.MessageAndOffset;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


/**
 * This function is serailized and pushed over the write to all executors
 */
public class SparkKafkaExecutorFunction implements VoidFunction<Iterator<Tuple2<byte[],byte[]>>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkKafkaExecutorFunction.class);
  private static volatile EmbeddedSDCPool sdcPool;
  private static final Object poolCreationLock = new Object();
  private Properties properties;
  private String pipelineJson;

  public SparkKafkaExecutorFunction(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson,  "Pipeline JSON");
  }

  private void initialize() throws Exception {
    synchronized (poolCreationLock) {
      if (sdcPool == null) {
        sdcPool = new EmbeddedSDCPool(properties, pipelineJson);
      }
    }
  }

  @Override
  public void call(Iterator<Tuple2<byte[], byte[]>> tupleIterator) throws Exception {
    LOG.debug("In kafka executor function " + " " + Thread.currentThread().getName());
    initialize();
    EmbeddedSDC embeddedSDC = sdcPool.getEmbeddedSDC();
    List<MessageAndPartition> batch = new ArrayList<>();
    while (tupleIterator.hasNext()) {
      Tuple2<byte[], byte[]> tuple = tupleIterator.next();
      // Get offset and partition from HasOffsetRange API
      batch.add(new MessageAndPartition(tuple._1(), tuple._2));
      LOG.debug("Got message: " + new String(tuple._1()) + " " + new String(tuple._2()));
    }
    embeddedSDC.getSource().put(batch);
  }

  public static void
    execute(Properties properties, String pipelineJson, Iterator<Tuple2<byte[], byte[]>> tupleIterator)
      throws Exception {
    SparkKafkaExecutorFunction function = new SparkKafkaExecutorFunction(properties, pipelineJson);
    function.call(tupleIterator);
  }

  public static long getRecordsProducedJVMWide() {
    long result = 0;
    if (sdcPool == null) {
      throw new RuntimeException("Embedded SDC pool is not initialized");
    }
    for (EmbeddedSDC sdc : sdcPool.getTotalInstances()) {
      result += ((SparkStreamingKafkaSource) sdc.getSource()).getRecordsProduced();
    }
    return result;
  }
}
