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
package com.streamsets.pipeline.stage.origin.jdbc.cdc;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SQLServerCDCSourceUpgrader;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSQLServerCDCSourceUpgrader {
  private static final String TABLECONFIG = "cdcTableJdbcConfigBean.tableConfigs";
  private static final String SCHEMA_CONFIG = "schema";
  private static final String TABLEPATTERN_CONFIG = "tablePattern";
  private static final String TABLE_EXCLUSION_CONFIG = "tableExclusionPattern";
  private static final String TABLE_INITIALOFFSET_CONFIG = "initialOffset";
  private static final String TABLE_CAPTURE_INSTANCE_CONFIG = "capture_instance";
  private static final String TABLE_TIMEZONE_ID = "cdcTableJdbcConfigBean.timeZoneID";

  @Test
  public void testUpgradeV1toV2WithBasicConfig() throws StageException {
    List<Config> configs = new ArrayList<>();
    List<Map<String, String>> oldTableConfigs = new ArrayList<>();

    final String schema1 = "dbo";
    final String table1 = "table1_%";

    Map<String, String> tableConfig1 = ImmutableMap.of(SCHEMA_CONFIG, schema1, TABLEPATTERN_CONFIG, table1);

    oldTableConfigs.add(tableConfig1);

    configs.add(new Config(TABLECONFIG, oldTableConfigs));

    Assert.assertEquals(1, configs.size());

    SQLServerCDCSourceUpgrader sqlServerCDCSourceUpgrader = new SQLServerCDCSourceUpgrader();
    sqlServerCDCSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(2, configs.size());

    // "Allow Late Table" config returns false
    Assert.assertEquals(false, configs.get(0).getValue());

    ArrayList<HashMap<String, String>> tableConfigs = (ArrayList<HashMap<String, String>>)configs.get(1).getValue();
    Assert.assertEquals(1, tableConfigs.size());

    HashMap<String, String> tableConfig = tableConfigs.get(0);

    Assert.assertEquals(schema1 + "_" + table1, tableConfig.get(TABLE_CAPTURE_INSTANCE_CONFIG));
    Assert.assertFalse(tableConfig.containsKey(TABLE_EXCLUSION_CONFIG));
    Assert.assertFalse(tableConfig.containsKey(TABLE_INITIALOFFSET_CONFIG));
  }

  @Test
  public void testUpgradeV1toV2WithFilledConfig() throws StageException {
    List<Config> configs = new ArrayList<>();
    List<Map<String, String>> oldTableConfigs = new ArrayList<>();

    final String schema1 = "dbo";
    final String table1 = "table1_%";
    final String exclusion1 = "exception-%";
    final String initalOffset1 = "0000";

    Map<String, String> tableConfig1 = ImmutableMap.of(
        SCHEMA_CONFIG, schema1,
        TABLEPATTERN_CONFIG, table1,
        TABLE_EXCLUSION_CONFIG, exclusion1,
        TABLE_INITIALOFFSET_CONFIG, initalOffset1
    );

    oldTableConfigs.add(tableConfig1);

    configs.add(new Config(TABLECONFIG, oldTableConfigs));

    Assert.assertEquals(1, configs.size());

    SQLServerCDCSourceUpgrader sqlServerCDCSourceUpgrader = new SQLServerCDCSourceUpgrader();
    sqlServerCDCSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(2, configs.size());

    // "Allow Late Table" config returns false
    Assert.assertEquals(false, configs.get(0).getValue());

    ArrayList<HashMap<String, String>> tableConfigs = (ArrayList<HashMap<String, String>>)configs.get(1).getValue();
    Assert.assertEquals(1, tableConfigs.size());

    HashMap<String, String> tableConfig = tableConfigs.get(0);

    Assert.assertEquals(schema1 + "_" + table1, tableConfig.get(TABLE_CAPTURE_INSTANCE_CONFIG));
    Assert.assertEquals(exclusion1, tableConfig.get(TABLE_EXCLUSION_CONFIG));
    Assert.assertEquals(initalOffset1, tableConfig.get(TABLE_INITIALOFFSET_CONFIG));
  }

  @Test
  public void testUpgradeV1toV3() throws StageException {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("cdcTableJdbcConfigBean.numberOfThreads", 4));
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    configs.add(new Config(queryIntervalField, "${10 * SECONDS}"));

    List<Map<String, String>> oldTableConfigs = new ArrayList<>();

    final String schema1 = "dbo";
    final String table1 = "table1_%";
    final String exclusion1 = "exception-%";
    final String initalOffset1 = "0000";

    Map<String, String> tableConfig1 = ImmutableMap.of(
        SCHEMA_CONFIG, schema1,
        TABLEPATTERN_CONFIG, table1,
        TABLE_EXCLUSION_CONFIG, exclusion1,
        TABLE_INITIALOFFSET_CONFIG, initalOffset1
    );

    oldTableConfigs.add(tableConfig1);

    // table config changes from V2
    configs.add(new Config(TABLECONFIG, oldTableConfigs));
    // removed timezone config from V3
    configs.add(new Config(TABLE_TIMEZONE_ID, ""));

    Assert.assertEquals(4, configs.size());

    SQLServerCDCSourceUpgrader sqlServerCDCSourceUpgrader = new SQLServerCDCSourceUpgrader();
    sqlServerCDCSourceUpgrader.upgrade("a", "b", "c", 1, 3, configs);

    Assert.assertEquals(4, configs.size());

    // Assertion for V2
    // "Allow Late Table" config returns false
    UpgraderTestUtils.assertExists(configs, SQLServerCDCSourceUpgrader.ALLOW_LATE_TABLE, false);

    Config tableConfigObj = UpgraderUtils.getConfigWithName(configs, SQLServerCDCSourceUpgrader.TABLECONFIG);
    ArrayList<HashMap<String, String>> tableConfigs = (ArrayList<HashMap<String, String>>) tableConfigObj.getValue();
    Assert.assertEquals(1, tableConfigs.size());

    HashMap<String, String> tableConfig = tableConfigs.get(0);

    Assert.assertEquals(schema1 + "_" + table1, tableConfig.get(TABLE_CAPTURE_INSTANCE_CONFIG));
    Assert.assertEquals(exclusion1, tableConfig.get(TABLE_EXCLUSION_CONFIG));
    Assert.assertEquals(initalOffset1, tableConfig.get(TABLE_INITIALOFFSET_CONFIG));

    // TABLE_TIMEZONE_ID is not in config list
    Assert.assertFalse(
        configs.stream()
        .filter(config -> config.getName().equals(TABLE_TIMEZONE_ID))
        .findAny()
        .isPresent()
    );

    UpgraderTestUtils.assertNoneExist(configs, queryIntervalField);
    UpgraderTestUtils.assertExists(configs, "commonSourceConfigBean.queriesPerSecond", "0.4");
  }

  @Test
  public void testUpgradeV3toV4() throws StageException {
    List<Config> configs = new ArrayList<>();
    List<Map<String, String>> oldTableConfigs = new ArrayList<>();

    final String exclusion1 = "exception-%";
    final String initalOffset1 = "0000";
    final String captureInstance = "dbo_table1_CT";

    Map<String, String> tableConfig1 = ImmutableMap.of(
        TABLE_CAPTURE_INSTANCE_CONFIG, captureInstance,
        TABLE_EXCLUSION_CONFIG, exclusion1,
        TABLE_INITIALOFFSET_CONFIG, initalOffset1
    );

    oldTableConfigs.add(tableConfig1);

    // table config changes from V2
    configs.add(new Config(TABLECONFIG, oldTableConfigs));

    Assert.assertEquals(1, configs.size());

    SQLServerCDCSourceUpgrader sqlServerCDCSourceUpgrader = new SQLServerCDCSourceUpgrader();
    sqlServerCDCSourceUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    Assert.assertEquals(1, configs.size());

    Config tableConfigObj = UpgraderUtils.getConfigWithName(configs, SQLServerCDCSourceUpgrader.TABLECONFIG);
    ArrayList<HashMap<String, String>> tableConfigs = (ArrayList<HashMap<String, String>>) tableConfigObj.getValue();
    Assert.assertEquals(1, tableConfigs.size());

    HashMap<String, String> tableConfig = tableConfigs.get(0);

    Assert.assertEquals("dbo_table1", tableConfig.get(TABLE_CAPTURE_INSTANCE_CONFIG));
    Assert.assertEquals(exclusion1, tableConfig.get(TABLE_EXCLUSION_CONFIG));
    Assert.assertEquals(initalOffset1, tableConfig.get(TABLE_INITIALOFFSET_CONFIG));
  }

  @Test
  public void testUpgradeV4toV5() throws StageException {
    List<Config> configs = new ArrayList<>();

    SQLServerCDCSourceUpgrader sqlServerCDCSourceUpgrader = new SQLServerCDCSourceUpgrader();
    sqlServerCDCSourceUpgrader.upgrade("a", "b", "c", 4, 5, configs);

    Assert.assertEquals(2, configs.size());

    UpgraderTestUtils.assertExists(configs, SQLServerCDCSourceUpgrader.TXN_WINDOW, "${1 * HOURS}");
    UpgraderTestUtils.assertExists(configs, SQLServerCDCSourceUpgrader.USE_TABLE, true);
  }
}
