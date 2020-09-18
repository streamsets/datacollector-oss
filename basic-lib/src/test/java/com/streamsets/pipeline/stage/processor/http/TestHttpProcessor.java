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
package com.streamsets.pipeline.stage.processor.http;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AbstractHttpStageTest;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
    "javax.management.*",
    "jdk.internal.reflect.*"
})
@PrepareForTest({HttpProcessor.class, JerseyClientUtil.class})
public class TestHttpProcessor extends AbstractHttpStageTest {

  public HttpProcessorConfig getConf(String url) {
    HttpProcessorConfig conf = new HttpProcessorConfig();
    conf.httpMethod = HttpMethod.GET;
    conf.outputField = "/output";
    conf.dataFormat = DataFormat.TEXT;
    conf.resourceUrl = url;
    conf.headerOutputLocation = HeaderOutputLocation.HEADER;
    return conf;
  }

  @Override
  public List<Stage.ConfigIssue> runStageValidation(String url) throws Exception {
    HttpProcessorConfig config = getConf("http://localhost:10000");
    config.client.useProxy = true;
    config.client.proxy.uri = url;

    HttpProcessor processor = PowerMockito.spy(new HttpProcessor(config));

    ProcessorRunner runner = new ProcessorRunner.Builder(HttpDProcessor.class, processor)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    return runner.runValidateConfigs();
  }
}
