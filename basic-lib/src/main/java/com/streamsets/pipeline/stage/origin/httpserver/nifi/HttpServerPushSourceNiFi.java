/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.httpserver.nifi;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AbstractHttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;
import com.streamsets.pipeline.stage.origin.httpserver.HttpServerPushSource;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class HttpServerPushSourceNiFi extends HttpServerPushSource {
  public HttpServerPushSourceNiFi(HttpConfigs httpConfigs, int maxRequestSizeMB, DataFormat dataFormat, DataParserFormatConfig dataFormatConfig) {
    super(httpConfigs, maxRequestSizeMB, dataFormat, dataFormatConfig);
  }

  @Override
  protected AbstractHttpReceiverServer getHttpReceiver() {
    return new HttpReceiverServer(getHttpConfigs(), getReceiver(), getErrorQueue());
  }
}
