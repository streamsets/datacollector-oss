/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import com.streamsets.pipeline.BootstrapSpark;

import com.streamsets.pipeline.Utils;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Properties;

public class BootstrapSparkKafkaFunction implements VoidFunction<Iterator<Tuple2<byte[],byte[]>>>, Serializable {

  private static volatile boolean initialized = false;
  private static Method sparkExecutorFunctionMethod;
  private Properties properties;
  private String pipelineJson;

  public BootstrapSparkKafkaFunction(Properties properties, String pipelineJson) {
    this.properties = Utils.checkNotNull(properties, "Properties");
    this.pipelineJson = Utils.checkNotNull(pipelineJson, "Pipeline JSON");
  }

  private static synchronized void initialize() throws Exception {
    if (initialized) {
      return;
    }
    sparkExecutorFunctionMethod = BootstrapSpark.getSparkKafkaExecutorFunction();
    initialized = true;
  }

  @Override
  public void call(Iterator<Tuple2<byte[],byte[]>> tupleIterator) throws Exception {
    BootstrapSparkKafkaFunction.initialize();
    sparkExecutorFunctionMethod.invoke(null, properties, pipelineJson, tupleIterator);
  }

}
