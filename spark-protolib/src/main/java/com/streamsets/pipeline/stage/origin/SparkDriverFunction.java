/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * This function executes in the driver.
 */
public class SparkDriverFunction implements Function<JavaRDD<String>, Void>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDriverFunction.class);
  private final EmbeddedSDCConf sdcConf;
  public SparkDriverFunction(EmbeddedSDCConf sdcConf) {
    this.sdcConf = sdcConf;
  }
  @Override
  public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
    stringJavaRDD.foreachPartition(new SparkExecutorFunction(sdcConf));
    return null;
  }


}
