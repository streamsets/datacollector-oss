/**
 *
 *  Copyright 2017 StreamSets Inc.
 *
 *  Licensed under the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.ClusterFunctionProvider;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.impl.ClusterFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkProcessorErrorMappingFunction implements VoidFunction<Iterator<Tuple2<Record, String>>> {

  private int id;
  private static final String ERROR_HEADER = "streamsetsInternalClusterErrorReason";

  private SparkProcessorErrorMappingFunction() {

  }

  public SparkProcessorErrorMappingFunction(int id) {
    this.id = id;
  }

  @Override
  public void call(Iterator<Tuple2<Record, String>> errors) throws Exception {
    ClusterFunction fn = ClusterFunctionProvider.getClusterFunction();
    List<Record> errorsToForward = new ArrayList<>();
    errors.forEachRemaining(x -> {
      x._1().getHeader().setAttribute(ERROR_HEADER, x._2());
      errorsToForward.add(x._1());
    });
    fn.writeErrorRecords(errorsToForward, id);
  }
}
