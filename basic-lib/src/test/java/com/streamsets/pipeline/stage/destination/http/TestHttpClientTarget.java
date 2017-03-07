/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AbstractHttpStageTest;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({HttpClientTarget.class, JerseyClientUtil.class})
public class TestHttpClientTarget extends AbstractHttpStageTest {

  public HttpClientTargetConfig getConf(String url) {
    HttpClientTargetConfig conf = new HttpClientTargetConfig();
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.TEXT;
    conf.resourceUrl = url;
    return conf;
  }

  @Override
  public List<Stage.ConfigIssue> runStageValidation(String url) throws Exception {
    HttpClientTargetConfig config = getConf("http://localhost:10000");
    config.client.useProxy = true;
    config.client.proxy.uri = url;

    HttpClientTarget processor = PowerMockito.spy(new HttpClientTarget(config));

    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    return runner.runValidateConfigs();
  }
}
