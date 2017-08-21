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

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  SPARK_00("Specified class: '{}' does not implement SparkTransformer interface"),
  SPARK_01("Specified class: '{}' was not found in classpath"),
  SPARK_02("Instantiating SparkTransformer class: '{}' with error: '{}'"),
  SPARK_03("Error accessing Streamsets directories"),
  SPARK_04("{}"),
  SPARK_05("Init method for SparkTransformer class: '{}' failed with error: '{}'"),
  SPARK_06("Error while transforming batch: {}"),
  SPARK_07("Spark job failed with error: {}"),
  SPARK_08("Parallelism must be >= 1"),
  ;

  private final String message;

  Errors(String message) {
    this.message = message;
  }
  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
