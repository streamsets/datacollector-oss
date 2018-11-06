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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestElasticSearchDTargetUpgrader {

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
}
