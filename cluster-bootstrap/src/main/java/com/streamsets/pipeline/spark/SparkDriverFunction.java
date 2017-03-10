/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Map;

/**
 * This function executes in the driver.
 */
public class SparkDriverFunction<T1, T2>  implements Function<JavaPairRDD<T1, T2>, Void>, Serializable {

  public SparkDriverFunction() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public Void call(JavaPairRDD<T1, T2> byteArrayJavaRDD) throws Exception {
    synchronized (BootstrapCluster.class) {
      Map<Integer, Long> offsets = KafkaOffsetUtil.getOffsets(byteArrayJavaRDD);
      DriverFunctionImpl.processRDD(byteArrayJavaRDD, new BootstrapSparkFunction());
      KafkaOffsetUtil.saveOffsets(offsets);
      return null;
    }
  }

}
