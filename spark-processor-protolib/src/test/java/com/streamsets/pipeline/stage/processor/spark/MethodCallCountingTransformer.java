/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.spark;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class MethodCallCountingTransformer extends SparkTransformer {

  static int initCallCount = 0;
  static int transformCallCount = 0;
  static int destroyCallCount = 0;

  public MethodCallCountingTransformer() {
    initCallCount = transformCallCount = destroyCallCount = 0;
  }

  @Override
  public void init(JavaSparkContext jsc, List<String> params) {
    initCallCount++;
  }

  @Override
  public TransformResult transform(JavaRDD<Record> recordRDD) {
    transformCallCount++;
    return new TransformResult(recordRDD, null);
  }

  @Override
  public void destroy() {
    destroyCallCount++;
  }
}
