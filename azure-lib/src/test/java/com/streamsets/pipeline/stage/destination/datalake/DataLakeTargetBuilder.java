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
package com.streamsets.pipeline.stage.destination.datalake;

import com.streamsets.pipeline.config.DataFormat;

public class DataLakeTargetBuilder {
  private DataLakeConfigBean conf;

  public DataLakeTargetBuilder() {
    conf = new DataLakeConfigBean();
    conf.clientId = () -> "1";
    conf.clientKey = () -> "dummy";
    conf.dirPathTemplate = "/tmp/out/";
    conf.uniquePrefix = "test";
    conf.fileNameSuffix = "";
    conf.timeDriver = "${time:now()}";
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textFieldPath = "/";
    conf.timeZoneID = "UTC";
  }

  public DataLakeTargetBuilder accountFQDN(String accountFQDN) {
    conf.accountFQDN = () -> accountFQDN;
    return this;
  }

  public DataLakeTargetBuilder authTokenEndpoint(String authTokenEndpoint) {
    conf.authTokenEndpoint = () -> authTokenEndpoint;
    return this;
  }

  public DataLakeTargetBuilder filesPrefix(String uniquePrefix) {
    conf.uniquePrefix = uniquePrefix;
    return this;
  }

  public DataLakeTarget build() {
    return new DataLakeTarget(conf);
  }

}
