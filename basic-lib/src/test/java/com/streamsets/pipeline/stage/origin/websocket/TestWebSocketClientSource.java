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
package com.streamsets.pipeline.stage.origin.websocket;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.destination.http.HttpClientTarget;
import com.streamsets.pipeline.stage.origin.websocketserver.WebSocketServerDPushSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({HttpClientTarget.class, JerseyClientUtil.class})
public class TestWebSocketClientSource {

  public WebSocketClientSourceConfigBean getConf(String url) {
    WebSocketClientSourceConfigBean conf = new WebSocketClientSourceConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.resourceUrl = url;
    return conf;
  }

  public List<Stage.ConfigIssue> runStageValidation(String url) throws Exception {
    WebSocketClientSourceConfigBean config = getConf(url);

    WebSocketClientSource webSocketClientSource = PowerMockito.spy(new WebSocketClientSource(config));

    PushSourceRunner runner = new PushSourceRunner.Builder(WebSocketServerDPushSource.class, webSocketClientSource)
        .addOutputLane("a")
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
