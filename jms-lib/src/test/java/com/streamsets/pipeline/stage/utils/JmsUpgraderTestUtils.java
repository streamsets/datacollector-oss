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
package com.streamsets.pipeline.stage.utils;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JmsUpgraderTestUtils {

  public static void testConnectionIntroducionUpgrade(String prefix, int fromVersion, StageUpgrader upgrader, List<Config> configs) {

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(fromVersion).when(context).getFromVersion();
    Mockito.doReturn(fromVersion+1).when(context).getToVersion();

    configs.add(new Config(prefix+".initialContextFactory", "v1"));
    configs.add(new Config(prefix+".connectionFactory", "v2"));
    configs.add(new Config(prefix+".providerURL", "v3"));
    configs.add(new Config("credentialsConfig.useCredentials", true));
    configs.add(new Config("credentialsConfig.username", "v5"));
    configs.add(new Config("credentialsConfig.password", "v6"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, prefix+".connection.initialContextFactory", "v1");
    UpgraderTestUtils.assertExists(configs, prefix+".connection.connectionFactory", "v2");
    UpgraderTestUtils.assertExists(configs, prefix+".connection.providerURL", "v3");
    UpgraderTestUtils.assertExists(configs, prefix+".connection.useCredentials", true);
    UpgraderTestUtils.assertExists(configs, prefix+".connection.username", "v5");
    UpgraderTestUtils.assertExists(configs, prefix+".connection.password", "v6");
    UpgraderTestUtils.assertExists(configs, prefix+".connection.additionalSecurityProps");

    Assert.assertEquals(7, configs.size());
  }

}
