/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

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
public class SparkDriverFunction<T1, T2>  implements Function<JavaPairRDD<T1, T2>, Void>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDriverFunction.class);
  private Properties properties;

  public SparkDriverFunction(Properties properties) {
    this.properties = Utils.checkNotNull(properties, "Properties");
  }

  @Override
  public Void call(JavaPairRDD<T1, T2> byteArrayJavaRDD) throws Exception {
    byteArrayJavaRDD.foreachPartition(new BootstrapSparkFunction(properties));
    return null;
  }
}
