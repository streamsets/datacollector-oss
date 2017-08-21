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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AbstractHttpStageTest;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpClientSource.class, JerseyClientUtil.class})
public class TestHttpClientSource extends AbstractHttpStageTest {

  public HttpClientConfigBean getConf(String url) {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = url;
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    return conf;
  }

  @Override
  public List<Stage.ConfigIssue> runStageValidation(String uri) throws Exception {
    HttpClientConfigBean conf = getConf("http://localhost:10000");
    conf.client.useProxy = true;
    conf.client.proxy.uri = uri;

    HttpClientSource origin = PowerMockito.spy(new HttpClientSource(conf));

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    return runner.runValidateConfigs();
  }

}
