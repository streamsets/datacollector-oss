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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  INFLUX_01("Database '{}' not found."),
  INFLUX_02("Record root is expected to be a Map, but was a '{}'"),
  INFLUX_03("Record is missing collectd plugin field."),
  INFLUX_04("Record is missing either a 'time' or 'time_hires' field."),
  INFLUX_05("Unrecognized Record Converter type: '{}'"),
  INFLUX_06("Record doesn't contain any values."),
  INFLUX_07("Record is missing the specified field '{}'"),
  INFLUX_08("The specified tag field '{}' is a List. It must be either a Map or a value representable by a string."),
  INFLUX_09("The field '{}' contains a '{}' which cannot be used for the time value."),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
