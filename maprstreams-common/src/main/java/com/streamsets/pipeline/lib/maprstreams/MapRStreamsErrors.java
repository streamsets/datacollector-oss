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
package com.streamsets.pipeline.lib.maprstreams;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum MapRStreamsErrors implements ErrorCode {

  // MapR Streams Common
  MAPRSTREAMS_01("Error getting metadata for topic '{}' from MapR Streams. Reason: {}"),
  MAPRSTREAMS_02("Cannot find metadata for topic '{}' from MapR Streams"),


  // MapR Streams Producer
  MAPRSTREAMS_20("Error writing data to the MapR Streams: {}"),
  MAPRSTREAMS_21("Wrong format of topic name: {}"),
  MAPRSTREAMS_22("Failed to create a stream '{}' : "),
  MAPRSTREAMS_23("Operation failed while getting streams data : {}"),

  // MapR Streams Consumer

  ;

  private final String msg;

  MapRStreamsErrors(String msg) {
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
