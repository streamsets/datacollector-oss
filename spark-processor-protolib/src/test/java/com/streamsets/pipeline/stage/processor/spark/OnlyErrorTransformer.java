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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class OnlyErrorTransformer extends SparkTransformer implements Serializable {

  public static final String ERROR_PATH = "/err";

  @Override
  public void init(JavaSparkContext javaSparkContext, List<String> params) {
  }

  @Override
  public TransformResult transform(JavaRDD<Record> javaRDD) {
    JavaPairRDD<Record, String> errors = javaRDD.mapToPair(new PairFunction<Record, Record, String>() {
      @Override
      public Tuple2<Record, String> call(Record record) throws Exception {
        return new Tuple2<>(record, record.get(ERROR_PATH).getValueAsString());
      }
    });

    JavaRDD<Record> result = null;
    return new TransformResult(result, errors);
  }
}
