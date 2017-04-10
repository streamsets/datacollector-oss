/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.BootstrapCluster;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DriverFunctionImpl {

  private static List<SparkTransformer> transformers;
  private static Optional<JavaPairRDD> previousIncomingData = Optional.empty();
  private static Deque<JavaRDD> previousBatch = new LinkedList<>();

  private DriverFunctionImpl() {

  }


  public static void setTransformers(List<SparkTransformer> transformers) throws Exception {
    DriverFunctionImpl.transformers = transformers;
  }

  public static <T1, T2> void processRDD(
      JavaPairRDD<T1, T2> byteArrayJavaRDD,
      AbstractBootstrapSparkFunction<T1, T2> fn
  ) throws Exception {
    previousBatch.forEach(rdd -> rdd.unpersist(false)); // clean up old RDDs.
    previousBatch.clear();

    previousIncomingData.ifPresent(JavaPairRDD::unpersist);

    byteArrayJavaRDD.cache();
    previousIncomingData = Optional.of(byteArrayJavaRDD);
    long count = byteArrayJavaRDD.count();
    if (count == 0) {
      return;
    }

    if (transformers == null) {
      setTransformers(BootstrapCluster.getTransformers());
    }
    int partitionCount = byteArrayJavaRDD.partitions().size();

    JavaRDD<Record> nextResult = byteArrayJavaRDD.mapPartitions(fn);
    previousBatch.addFirst(nextResult);

    nextResult.cache();
    nextResult.count();
    if (transformers != null && !transformers.isEmpty()) {
      for (int i = 0; i < transformers.size(); i++) {
        SparkTransformer transformer = transformers.get(i);
        TransformResult result = transformer.transform(nextResult);
        JavaPairRDD<Record, String> errors = result.getErrors();

        if (errors.partitions().size() > partitionCount) { // Avoid shuffle
          errors = errors.coalesce(partitionCount);
        } else if (errors.partitions().size() < partitionCount) {
          errors = errors.repartition(partitionCount);
        }

        errors.foreachPartition(new SparkProcessorErrorMappingFunction(i));

        JavaRDD<Record> resultRDD = result.getResult();

        if (resultRDD.partitions().size() > partitionCount) {
          resultRDD = resultRDD.coalesce(partitionCount);
        } else if (nextResult.partitions().size() < partitionCount) {
          resultRDD = resultRDD.repartition(partitionCount);
        }

        nextResult = resultRDD.mapPartitions(new SparkProcessorMappingFunction(i));
        nextResult.cache();
        nextResult.count();
        previousBatch.addFirst(nextResult);
      }
    }

    // nextResult can never be null, since it is the result of a mapPartitions call.
    // materialize the RDD.
    nextResult.count();
  }
}
