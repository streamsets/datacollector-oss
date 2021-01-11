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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestElasticSearchDSourceUpgrader {

  private StageUpgrader elasticSearchSourceUpgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/ElasticsearchDSource.yaml");
    elasticSearchSourceUpgrader = new SelectorStageUpgrader("stage", new ElasticSearchDSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV2() throws StageException {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    String securityPrefix = "conf.securityConfig";
    configs.add(new Config(securityPrefix + ".securityUser", "username:password"));
    configs = elasticSearchSourceUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, securityPrefix + ".securityUser", "username:password");
    UpgraderTestUtils.assertExists(configs, securityPrefix + ".securityPassword", "");
  }

  @Test
  public void testV2ToV3WithSSL() throws StageException {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs.add(new Config("conf.httpUris", Arrays.asList(
        "localhost:80",
        "https://localhost:443"
    )));
    configs.add(new Config("conf.useSecurity", true));
    configs.add(new Config("conf.securityConfig.securityMode", "param1"));
    configs.add(new Config("conf.securityConfig.awsRegion", "param2"));
    configs.add(new Config("conf.securityConfig.endpoint", "param3"));
    configs.add(new Config("conf.securityConfig.awsAccessKeyId", "param4"));
    configs.add(new Config("conf.securityConfig.awsSecretAccessKey", "param5"));
    configs.add(new Config("conf.securityConfig.securityUser", "param6"));
    configs.add(new Config("conf.securityConfig.securityPassword", "param7"));
    configs.add(new Config("conf.securityConfig.sslTrustStorePath", "param8"));
    configs.add(new Config("conf.securityConfig.sslTrustStorePassword", "param9"));
    configs = elasticSearchSourceUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.connection.serverUrl", "localhost:80,https://localhost:443");
    UpgraderTestUtils.assertExists(configs, "conf.connection.port", "9200");
    UpgraderTestUtils.assertExists(configs, "conf.connection.useSecurity", true);
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityMode", "param1");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsRegion", "param2");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.endpoint", "param3");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsAccessKeyId", "param4");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsSecretAccessKey", "param5");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityUser", "param6");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityPassword", "param7");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.enableSSL", true);
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.sslTrustStorePath", "param8");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.sslTrustStorePassword", "param9");
  }

  @Test
  public void testV2ToV3WithoutSSL() throws StageException {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs.add(new Config("conf.httpUris", Arrays.asList(
        "localhost:80",
        "https://localhost:443"
    )));
    configs.add(new Config("conf.useSecurity", true));
    configs.add(new Config("conf.securityConfig.securityMode", "param1"));
    configs.add(new Config("conf.securityConfig.awsRegion", "param2"));
    configs.add(new Config("conf.securityConfig.endpoint", "param3"));
    configs.add(new Config("conf.securityConfig.awsAccessKeyId", "param4"));
    configs.add(new Config("conf.securityConfig.awsSecretAccessKey", "param5"));
    configs.add(new Config("conf.securityConfig.securityUser", "param6"));
    configs.add(new Config("conf.securityConfig.securityPassword", "param7"));
    configs.add(new Config("conf.securityConfig.sslTrustStorePath", ""));
    configs.add(new Config("conf.securityConfig.sslTrustStorePassword", ""));
    configs = elasticSearchSourceUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.connection.serverUrl", "localhost:80,https://localhost:443");
    UpgraderTestUtils.assertExists(configs, "conf.connection.port", "9200");
    UpgraderTestUtils.assertExists(configs, "conf.connection.useSecurity", true);
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityMode", "param1");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsRegion", "param2");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.endpoint", "param3");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsAccessKeyId", "param4");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.awsSecretAccessKey", "param5");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityUser", "param6");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.securityPassword", "param7");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.enableSSL", false);
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.sslTrustStorePath", "");
    UpgraderTestUtils.assertExists(configs, "conf.connection.securityConfig.sslTrustStorePassword", "");
  }
}
