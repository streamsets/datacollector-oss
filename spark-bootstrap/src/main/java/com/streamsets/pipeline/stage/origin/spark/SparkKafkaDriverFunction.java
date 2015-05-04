/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.streamsets.pipeline.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * This function executes in the driver.
 */
public class SparkKafkaDriverFunction implements Function<JavaPairRDD<byte[], byte[]>, Void>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkKafkaDriverFunction.class);
  private Properties properties;
  private String pipelineJson;
  public SparkKafkaDriverFunction(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson, "Pipeline JSON");
    System.err.println("SparkDriverFunction.<init>");
    Thread.dumpStack();
  }

  @Override
  public Void call(JavaPairRDD<byte[], byte[]> byteArrayJavaRDD) throws Exception {
    System.err.println("SparkDriverFunction.call");
    Thread.dumpStack();
    byteArrayJavaRDD.foreachPartition(new BootstrapSparkKafkaFunction(properties, pipelineJson));
    return null;
  }
}
