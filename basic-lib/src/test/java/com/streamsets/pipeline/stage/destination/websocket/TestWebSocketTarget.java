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
package com.streamsets.pipeline.stage.destination.websocket;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.http.HttpClientDTarget;
import com.streamsets.pipeline.stage.destination.http.HttpClientTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.junit.Assert;
import org.junit.Test;
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
@PrepareForTest({HttpClientTarget.class, JerseyClientUtil.class})
public class TestWebSocketTarget {

  public WebSocketTargetConfig getConf(String url) {
    WebSocketTargetConfig conf = new WebSocketTargetConfig();
    conf.dataFormat = DataFormat.TEXT;
    conf.resourceUrl = url;
    conf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    conf.dataGeneratorFormatConfig.textFieldPath = "/text";
    return conf;
  }

  public List<Stage.ConfigIssue> runStageValidation(String url) throws Exception {
    WebSocketTargetConfig config = getConf(url);

    WebSocketTarget processor = PowerMockito.spy(new WebSocketTarget(config));

    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    return runner.runValidateConfigs();
  }

  public void checkInvalidURLError(String url) throws Exception {
    List<Stage.ConfigIssue> issues = runStageValidation(url);
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testResourceUrlWithoutProtocol() throws Exception {
    checkInvalidURLError("localhost:10000");
  }

  @Test
  public void testResourceUrlWithNoUrlInfo() throws Exception {
    checkInvalidURLError("ws://");
  }

  public void checkValidURL(String url) throws Exception {
    List<Stage.ConfigIssue> issues = runStageValidation(url);
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testValidResourceUrl() throws Exception {
    checkValidURL("ws://localhost:8000");
    checkValidURL("WS://localhost:8000");
    checkValidURL("wss://localhost:8000");
    checkValidURL("WSS://localhost:8000");

  }
}
