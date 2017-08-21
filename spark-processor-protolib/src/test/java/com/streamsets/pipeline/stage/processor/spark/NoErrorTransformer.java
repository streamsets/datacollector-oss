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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.List;

public class NoErrorTransformer extends SparkTransformer implements Serializable{

  public static final String VALUE_PATH = "/text";
  public static final String MAPPED = "/mapped";
  public static final String CONSTANT = "/constant";
  private int incrementValue = 1000;
  private String constantValue = "STREAMSETS";

  @Override
  public void init(JavaSparkContext javaSparkContext, List<String> params) {
    incrementValue = Integer.parseInt(params.get(0));
    constantValue = params.get(1);
  }

  @Override
  public TransformResult transform(JavaRDD<Record> javaRDD) {
    JavaPairRDD<Record, String> errors = null;

    JavaRDD<Record> result = javaRDD.map(new Function<Record, Record>() {
      public Record call(Record v1) throws Exception {
        v1.set(MAPPED, Field.create(v1.get(VALUE_PATH).getValueAsInteger() + incrementValue));
        v1.set(CONSTANT, Field.create(constantValue));
        return v1;
      }
    });
    return new TransformResult(result, errors);
  }
}
