/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.conf;

public enum DataLakeConnectionProtocol  {

  // Gen2
  ABFS_PROTOCOL("abfs://"),
  ABFS_PROTOCOL_SECURE("abfss://"),

  // Gen1
  ADL_PROTOCOL_SECURE("adl://"),
  ;

  private final String protocol;

  DataLakeConnectionProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getProtocol() {
    return this.protocol;
  }
}
