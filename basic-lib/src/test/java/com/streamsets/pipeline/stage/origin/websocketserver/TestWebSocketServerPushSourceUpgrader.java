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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestWebSocketServerPushSourceUpgrader {

  @Test
  public void testV1ToV10() throws Exception {
    TlsConfigBeanUpgraderTestUtil.testRawKeyStoreConfigsToTlsConfigBeanUpgrade(
        "webSocketConfigs.",
        new WebSocketServerPushSourceUpgrader(),
        10
    );
  }

  @Test
  public void testV10ToV11() throws Exception {
    List<Config> configs = new LinkedList<>();
    WebSocketServerPushSourceUpgrader upgrader = new WebSocketServerPushSourceUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("l", "s", "i", 10, 11);
    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "responseConfig.dataFormat",
        "responseConfig.dataGeneratorFormatConfig.charset",
        "responseConfig.dataGeneratorFormatConfig.jsonMode"
    );
  }

  @Test
  public void testV11ToV12() throws Exception {
    List<Config> configs = new LinkedList<>();
    WebSocketServerPushSourceUpgrader upgrader = new WebSocketServerPushSourceUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("l", "s", "i", 11, 12);
    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "responseConfig.sendRawResponse"
    );
  }
}
