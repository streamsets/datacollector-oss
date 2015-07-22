/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * This function executes in the driver.
 */
public class SparkDriverFunction<T1, T2>  implements Function<JavaPairRDD<T1, T2>, Void>, Serializable {

  public SparkDriverFunction() {
  }

  @Override
  public Void call(JavaPairRDD<T1, T2> byteArrayJavaRDD) throws Exception {
    byteArrayJavaRDD.foreachPartition(new BootstrapSparkFunction());
    return null;
  }
}
