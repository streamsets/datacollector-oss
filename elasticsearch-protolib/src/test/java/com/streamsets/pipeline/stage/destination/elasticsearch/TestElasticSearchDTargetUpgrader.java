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
package com.streamsets.pipeline.stage.destination.elasticsearch;

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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestElasticSearchDTargetUpgrader {

  private StageUpgrader elasticSearchTargetUpgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Test
  @SuppressWarnings("unchecked")
  public void testUpgrader() throws Exception {
    StageUpgrader upgrader = new ElasticsearchDTargetUpgrader();

    List<Config> configs = createConfigs();

    List<Config> newConfigs = upgrader.upgrade("l", "s", "i", 1, 6, configs);

    assertEquals(6, configs.size());
    assertEquals("elasticSearchConfigBean.timeDriver", newConfigs.get(0).getName());
    assertEquals("elasticSearchConfigBean.timeZoneID", newConfigs.get(1).getName());
    assertEquals("elasticSearchConfigBean.httpUris", newConfigs.get(2).getName());
    assertEquals("http://localhost:9300", ((List<String>)newConfigs.get(2).getValue()).get(0));
    assertEquals("elasticSearchConfigBean.useSecurity", newConfigs.get(3).getName());
    assertEquals("elasticSearchConfigBean.params", newConfigs.get(4).getName());
    assertEquals("elasticSearchConfigBean.defaultOperation", newConfigs.get(5).getName());
  }

  private List<Config> createConfigs() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "clusterName", "MyCluster"));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "uris", Collections.EMPTY_LIST));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "httpUri", "http://localhost:9300"));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "useShield", false));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "useFound", false));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "configs", Collections.EMPTY_MAP));
    configs.add(new Config(ElasticsearchDTargetUpgrader.OLD_CONFIG_PREFIX + "upsert", false));

    return configs;
  }

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/ElasticSearchDTarget.yaml");
    elasticSearchTargetUpgrader = new SelectorStageUpgrader("stage", new ElasticsearchDTargetUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV7ToV8() throws StageException {
    StageUpgrader upgrader = new ElasticsearchDTargetUpgrader();
    List<Config> configs = createConfigs();
    List<Config> newConfigs = upgrader.upgrade("library", "stageName", "stageInstance", 2, 8, configs);
    UpgraderTestUtils.assertAllExist(newConfigs,
        "elasticSearchConfig.parentIdTemplate",
        "elasticSearchConfig.routingTemplate"
    );
  }

  @Test
  public void testV8ToV9() throws StageException {
    StageUpgrader upgrader = new ElasticsearchDTargetUpgrader();
    List<Config> configs = createConfigs();
    List<Config> newConfigs = upgrader.upgrade("library", "stageName", "stageInstance", 2, 9, configs);
    UpgraderTestUtils.assertAllExist(newConfigs,
        "elasticSearchConfig.securityConfig.securityMode",
        "elasticSearchConfig.securityConfig.awsRegion"
    );
  }

  @Test
  public void testV9ToV10() throws StageException {
    StageUpgrader upgrader = new ElasticsearchDTargetUpgrader();
    List<Config> configs = createConfigs();
    List<Config> newConfigs = upgrader.upgrade("library", "stageName", "stageInstance", 2, 10, configs);
    UpgraderTestUtils.assertAllExist(newConfigs,
        "elasticSearchConfig.rawAdditionalProperties"
    );
  }

  @Test
  public void testV10ToV11() throws StageException {
    Mockito.doReturn(10).when(context).getFromVersion();
    Mockito.doReturn(11).when(context).getToVersion();

    String securityPrefix = "elasticSearchConfig.securityConfig";
    configs.add(new Config(securityPrefix + ".securityUser", "username:password"));
    configs = elasticSearchTargetUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, securityPrefix + ".securityUser", "username:password");
    UpgraderTestUtils.assertExists(configs, securityPrefix + ".securityPassword", "");
  }

  @Test
  public void testV11ToV12WithSSL() throws StageException {
    Mockito.doReturn(11).when(context).getFromVersion();
    Mockito.doReturn(12).when(context).getToVersion();

    configs.add(new Config("elasticSearchConfig.httpUris", Arrays.asList(
        "localhost:80",
        "https://localhost:443"
    )));
    configs.add(new Config("elasticSearchConfig.useSecurity", true));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityMode", "param1"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsRegion", "param2"));
    configs.add(new Config("elasticSearchConfig.securityConfig.endpoint", "param3"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsAccessKeyId", "param4"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsSecretAccessKey", "param5"));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityUser", "param6"));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityPassword", "param7"));
    configs.add(new Config("elasticSearchConfig.securityConfig.sslTrustStorePath", "param8"));
    configs.add(new Config("elasticSearchConfig.securityConfig.sslTrustStorePassword", "param9"));
    configs = elasticSearchTargetUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.serverUrl", "localhost:80,https://localhost:443");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.port", "9200");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.useSecurity", true);
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityMode", "param1");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsRegion", "param2");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.endpoint", "param3");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsAccessKeyId", "param4");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsSecretAccessKey", "param5");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityUser", "param6");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityPassword", "param7");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.enableSSL", true);
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.sslTrustStorePath", "param8");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.sslTrustStorePassword", "param9");
  }

  @Test
  public void testV11ToV12WithoutSSL() throws StageException {
    Mockito.doReturn(11).when(context).getFromVersion();
    Mockito.doReturn(12).when(context).getToVersion();

    configs.add(new Config("elasticSearchConfig.httpUris", Arrays.asList(
        "localhost:80",
        "https://localhost:443"
    )));
    configs.add(new Config("elasticSearchConfig.useSecurity", true));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityMode", "param1"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsRegion", "param2"));
    configs.add(new Config("elasticSearchConfig.securityConfig.endpoint", "param3"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsAccessKeyId", "param4"));
    configs.add(new Config("elasticSearchConfig.securityConfig.awsSecretAccessKey", "param5"));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityUser", "param6"));
    configs.add(new Config("elasticSearchConfig.securityConfig.securityPassword", "param7"));
    configs.add(new Config("elasticSearchConfig.securityConfig.sslTrustStorePath", ""));
    configs.add(new Config("elasticSearchConfig.securityConfig.sslTrustStorePassword", ""));
    configs = elasticSearchTargetUpgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.serverUrl", "localhost:80,https://localhost:443");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.port", "9200");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.useSecurity", true);
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityMode", "param1");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsRegion", "param2");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.endpoint", "param3");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsAccessKeyId", "param4");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.awsSecretAccessKey", "param5");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityUser", "param6");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.securityPassword", "param7");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.enableSSL", false);
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.sslTrustStorePath", "");
    UpgraderTestUtils.assertExists(configs, "elasticSearchConfig.connection.securityConfig.sslTrustStorePassword", "");
  }
}
