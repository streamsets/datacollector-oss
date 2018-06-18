/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.splunk;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.stage.destination.http.HttpClientTarget;
import com.streamsets.pipeline.stage.destination.http.HttpClientTargetConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.ArrayList;
import java.util.List;

public class SplunkTarget extends HttpClientTarget {
  SplunkTargetConfig conf;
  HttpClientTargetConfig httpConf;

  SplunkTarget(SplunkTargetConfig conf, HttpClientTargetConfig httpConf) {
    super(httpConf);
    this.conf = conf;
    this.httpConf = httpConf;
  }

  protected List<ConfigIssue> init() {
    // Explicitly set configs that are important, even though they
    // may be defaults
    httpConf.dataFormat = DataFormat.JSON;
    httpConf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    httpConf.dataGeneratorFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;

    if (!conf.splunkEndpoint.endsWith("/")) {
      conf.splunkEndpoint += "/";
    }
    httpConf.resourceUrl = conf.splunkEndpoint + "services/collector";

    try {
      httpConf.headers.put("Authorization", "Splunk " + conf.splunkToken.get());
    } catch (StageException e) {
      List<ConfigIssue> issues = new ArrayList<>();

      issues.add(getContext().createConfigIssue(
          Groups.SPLUNK.name(),
          "conf.splunkToken",
          Errors.HTTP_29,
          "Splunk Token",
          e.toString()));
      return issues;
    }

    httpConf.httpMethod = HttpMethod.POST;
    httpConf.singleRequestPerBatch = true;
    httpConf.client.numThreads = 1;

    httpConf.client.transferEncoding = conf.client.transferEncoding;
    httpConf.client.httpCompression = conf.client.httpCompression;
    httpConf.client.connectTimeoutMillis = conf.client.connectTimeoutMillis;
    httpConf.client.readTimeoutMillis = conf.client.readTimeoutMillis;
    httpConf.client.useProxy = conf.client.useProxy;
    httpConf.client.proxy = conf.client.proxy;
    httpConf.client.tlsConfig = conf.client.tlsConfig;
    httpConf.client.requestLoggingConfig = conf.client.requestLoggingConfig;

    return super.init();
  }
}
