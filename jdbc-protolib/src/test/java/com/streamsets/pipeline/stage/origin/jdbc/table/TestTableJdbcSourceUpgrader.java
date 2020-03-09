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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class TestTableJdbcSourceUpgrader {

  @Test
  public void testUpgradeV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>();
    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs =
        upgrader.upgrade("a", "b", "c", 1, 2, configs);
    Assert.assertEquals(4, upgradedConfigs.size());
    for (Config config : upgradedConfigs) {
      if (config.getName().equals(TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX
          + TableJdbcConfigBean.BATCHES_FROM_THE_RESULT_SET)) {
        Assert.assertEquals(-1, config.getValue());
      } else if (config.getName().equals(TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX
          + TableJdbcConfigBean.QUOTE_CHAR)) {
        Assert.assertEquals(QuoteChar.NONE, config.getValue());
      } else if (config.getName().equals(TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX
          + TableJdbcConfigBean.NUMBER_OF_THREADS)) {
        Assert.assertEquals(1, config.getValue());
      } else if (config.getName().equals(CommonSourceConfigBean.COMMON_SOURCE_CONFIG_BEAN_PREFIX
          + CommonSourceConfigBean.NUM_SQL_ERROR_RETRIES)){
        Assert.assertEquals(0, config.getValue());
      } else {
        Assert.fail(Utils.format("Unexpected Config '{}' after upgrade", config.getName()));
      }
    }
  }

  @Test
  public void testUpgradeV2ToV3() throws Exception {

    List<Config> configs = new ArrayList<>();

    List<LinkedHashMap<String, Object>> tableConfigMaps = new LinkedList<>();

    LinkedHashMap<String, Object> tableConfigMap1 = new LinkedHashMap<>();
    tableConfigMap1.put("tablePattern", "pattern1");
    tableConfigMap1.put("overrideDefaultOffsetColumns", false);
    tableConfigMap1.put("offsetColumns", Collections.emptyList());
    tableConfigMap1.put("offsetColumnToInitialOffsetValue", Collections.emptyList());
    tableConfigMap1.put("schema", "schema");
    tableConfigMaps.add(tableConfigMap1);
    LinkedHashMap<String, Object> tableConfigMap2 = new LinkedHashMap<>(tableConfigMap1);
    tableConfigMap2.put("tablePattern", "pattern2");
    tableConfigMap2.put("schema", "schema2");
    tableConfigMaps.add(tableConfigMap2);

    configs.add(new Config(TableJdbcConfigBean.TABLE_CONFIG, tableConfigMaps));


    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);

    Config upgradedTableConfigs = UpgraderUtils.getConfigWithName(upgradedConfigs, TableJdbcConfigBean.TABLE_CONFIG);
    assertThat(upgradedTableConfigs, notNullValue());
    assertThat(upgradedTableConfigs.getValue(), is(instanceOf(List.class)));
    List<LinkedHashMap<String, Object>> upgradedTableConfigsList =
        (List<LinkedHashMap<String, Object>>) upgradedTableConfigs.getValue();

    assertThat(upgradedTableConfigsList, hasSize(2));
    LinkedHashMap<String, Object> upgradedTableConfig1 = upgradedTableConfigsList.get(0);
    LinkedHashMap<String, Object> upgradedTableConfig2 = upgradedTableConfigsList.get(1);
    assertAllContain(
        TableConfigBean.PARTITION_SIZE_FIELD,
        TableConfigBean.DEFAULT_PARTITION_SIZE,
        upgradedTableConfig1,
        upgradedTableConfig2
    );
    assertAllContain(
        TableConfigBean.PARTITIONING_MODE_FIELD,
        "DISABLED",
        upgradedTableConfig1,
        upgradedTableConfig2
    );
    assertAllContain(
        TableConfigBean.MAX_NUM_ACTIVE_PARTITIONS_FIELD,
        TableConfigBean.DEFAULT_MAX_NUM_ACTIVE_PARTITIONS,
        upgradedTableConfig1,
        upgradedTableConfig2
    );

    assertHasAllEntries(upgradedTableConfig1, tableConfigMap1);
    assertHasAllEntries(upgradedTableConfig2, tableConfigMap2);
  }

  @Test
  public void testUpgradeV3ToV4() throws Exception {

    List<Config> configs = new ArrayList<>();

    List<LinkedHashMap<String, Object>> tableConfigMaps = new LinkedList<>();

    LinkedHashMap<String, Object> tableConfigMap1 = new LinkedHashMap<>();
    tableConfigMap1.put("tablePattern", "pattern1");
    tableConfigMap1.put("overrideDefaultOffsetColumns", false);
    tableConfigMap1.put("offsetColumns", Collections.emptyList());
    tableConfigMap1.put("offsetColumnToInitialOffsetValue", Collections.emptyList());
    tableConfigMap1.put("schema", "schema");
    tableConfigMaps.add(tableConfigMap1);
    LinkedHashMap<String, Object> tableConfigMap2 = new LinkedHashMap<>(tableConfigMap1);
    tableConfigMap2.put("tablePattern", "pattern2");
    tableConfigMap2.put("schema", "schema2");
    tableConfigMaps.add(tableConfigMap2);

    configs.add(new Config(TableJdbcConfigBean.TABLE_CONFIG, tableConfigMaps));

    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);

    Config upgradedTableConfigs = UpgraderUtils.getConfigWithName(upgradedConfigs, TableJdbcConfigBean.TABLE_CONFIG);
    assertThat(upgradedTableConfigs, notNullValue());
    assertThat(upgradedTableConfigs.getValue(), is(instanceOf(List.class)));
    List<LinkedHashMap<String, Object>> upgradedTableConfigsList =
        (List<LinkedHashMap<String, Object>>) upgradedTableConfigs.getValue();

    assertThat(upgradedTableConfigsList, hasSize(2));
    LinkedHashMap<String, Object> upgradedTableConfig1 = upgradedTableConfigsList.get(0);
    LinkedHashMap<String, Object> upgradedTableConfig2 = upgradedTableConfigsList.get(1);
    assertAllContain(
        TableConfigBean.ENABLE_NON_INCREMENTAL_FIELD,
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        upgradedTableConfig1,
        upgradedTableConfig2
    );

    assertHasAllEntries(upgradedTableConfig1, tableConfigMap1);
    assertHasAllEntries(upgradedTableConfig2, tableConfigMap2);
  }

  @Test
  public void testUpgradeV4ToV5() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("tableJdbcConfigBean.numberOfThreads", 2));
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    configs.add(new Config(queryIntervalField, "${10 * SECONDS}"));

    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 4, 5, configs);

    UpgraderTestUtils.assertNoneExist(upgradedConfigs, queryIntervalField);
    UpgraderTestUtils.assertExists(upgradedConfigs, "commonSourceConfigBean.queriesPerSecond", "0.2");
  }

  @Test
  public void testUpgradeV4ToV5StringNumThreads() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("tableJdbcConfigBean.numberOfThreads", "${runtime:conf('numCpus')}"));
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    configs.add(new Config(queryIntervalField, "${10 * SECONDS}"));

    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 4, 5, configs);

    UpgraderTestUtils.assertNoneExist(upgradedConfigs, queryIntervalField);
    UpgraderTestUtils.assertExists(upgradedConfigs, "commonSourceConfigBean.queriesPerSecond", "0.2");
  }

  @Test
  // SDC-8014: if numThreads/queryInterval is non-terminating, we should not throw ArithmeticException
  public void testUpgradeV4ToV5NonTerminating() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("tableJdbcConfigBean.numberOfThreads", 22));
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    configs.add(new Config(queryIntervalField, "${7 * SECONDS}"));

    TableJdbcSourceUpgrader upgrader = new TableJdbcSourceUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 4, 5, configs);

    UpgraderTestUtils.assertNoneExist(upgradedConfigs, queryIntervalField);
    UpgraderTestUtils.assertAllExist(upgradedConfigs, "commonSourceConfigBean.queriesPerSecond");
    Assert.assertTrue(upgradedConfigs.stream()
        .filter(config -> config.getName().equals("commonSourceConfigBean.queriesPerSecond"))
        .allMatch(config -> ((String) config.getValue()).startsWith("3.14285")));
  }

  private static void assertAllContain(String configKey, Object configValue, LinkedHashMap... tableConfigMaps) {
    for (LinkedHashMap<String, Object> tableConfigMap : tableConfigMaps) {
      assertThat(tableConfigMap, hasEntry(
          configKey,
          configValue
      ));
    }
  }

  private static void assertHasAllEntries(Map<String, Object> expectedMap, Map<String, Object> actualMap) {
    for (Map.Entry<String, Object> entry : expectedMap.entrySet()) {
      assertThat(actualMap, hasEntry(entry.getKey(), entry.getValue()));
    }
  }

}
