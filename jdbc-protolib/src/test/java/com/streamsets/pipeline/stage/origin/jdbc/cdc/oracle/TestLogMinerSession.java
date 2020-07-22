/*
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class TestLogMinerSession {

  @Test
  public void testBuildTableConditionTablePatterns() {
    List<SchemaTableConfigBean> tables = ImmutableList.of(
        createTableConfig("SDC", "TABLE1", ""),
        createTableConfig("SDC", "TABLE2", ""),
        createTableConfig("SDC", "%PATTERN1%", ""),
        createTableConfig("SDC", "P_TTERN2", "")
    );

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        "((SEG_OWNER = 'SDC' AND (TABLE_NAME LIKE '%PATTERN1%' OR TABLE_NAME LIKE 'P_TTERN2' OR " +
            "TABLE_NAME IN ('TABLE1','TABLE2'))))",
        condition
    );

  }

  @Test
  public void testBuildTableConditionSchemaPatterns() {
    List<SchemaTableConfigBean> tables = ImmutableList.of(
        createTableConfig("%SDC%", "TABLE1", ""),
        createTableConfig("_SYS_", "%PATTERN1%", ""),
        createTableConfig("_SYS_", "%PATTERN2%", "")
    );

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        "((SEG_OWNER LIKE '_SYS_' AND (TABLE_NAME LIKE '%PATTERN1%' OR TABLE_NAME LIKE '%PATTERN2%'))" +
            " OR (SEG_OWNER LIKE '%SDC%' AND (TABLE_NAME IN ('TABLE1'))))",
        condition
    );

  }

  @Test
  public void testBuildTableConditionBig() {
    List<SchemaTableConfigBean> tables = new ArrayList<>(1010);
    List<String> tableNames = new ArrayList<>(1010);
    tables.add(createTableConfig("SYS", "%PATTERN%", ""));
    for (int i = 0; i < 1010; i++) {
      String tableName = RandomStringUtils.randomAlphanumeric(5);
      tableNames.add(Utils.format("'{}'", tableName));
      tables.add(createTableConfig("SYS", tableName, ""));
    }

    LogMinerSession.Builder builder = new LogMinerSession.Builder(Mockito.mock(Connection.class), 19);
    String condition = builder.buildTablesCondition(tables);
    Assert.assertEquals(
        Utils.format(
            "((SEG_OWNER = 'SYS' AND (TABLE_NAME LIKE '%PATTERN%' OR TABLE_NAME IN ({}) OR TABLE_NAME IN ({}))))",
            String.join(",", tableNames.subList(0, 1000)),
            String.join(",", tableNames.subList(1000, 1010))
        ),
        condition
    );
  }

  private SchemaTableConfigBean createTableConfig(String schema, String table, String exclusion) {
    SchemaTableConfigBean config = new SchemaTableConfigBean();
    config.schema = schema;
    config.table = table;
    config.excludePattern = exclusion;
    return config;
  }
}
