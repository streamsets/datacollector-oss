/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Properties;

/**
 * This function executes in the driver.
 */
public class SparkDriverFunction implements Function<JavaRDD<String>, Void>, Serializable {
  private Properties properties;
  private String pipelineJson;

  public SparkDriverFunction(Properties properties, String pipelineJson) {
    this.properties = properties;
    this.pipelineJson = pipelineJson;
    System.err.println("SparkDriverFunction.<init>");
    Thread.dumpStack();
  }
  @Override
  public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
    System.err.println("SparkDriverFunction.call");
    Thread.dumpStack();
    stringJavaRDD.foreachPartition(new BootstrapSparkFunction(properties, pipelineJson));
    return null;
  }


}
