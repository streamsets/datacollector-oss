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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestTableExclusion {
  private static final String USER_NAME = "sa";
  private static final String PASSWORD = "sa";
  protected static final String SCHEMA = "TEST";
  private static final String JDBC_URL = "jdbc:h2:mem:" + SCHEMA;
  private static final String CREATE_TABLE_PATTERN = "CREATE TABLE IF NOT EXISTS %s.%s (p_id INT NOT NULL PRIMARY KEY);";
  private static final String DELETE_TABLE_PATTERN = "DROP TABLE IF EXISTS %s.%s;";

  private static final Set<String> TABLE_NAMES =
      ImmutableSet.of(
          "TABLEA", "TABLEB", "TABLEC", "TABLED", "TABLEE",
          "TABLE1", "TABLE2", "TABLE3", "TABLE4", "TABLE5"
      );

  private static final Map<String, List<String>> multipleSchemaTables = new HashMap<>();

  private static final String MULTISCHEMA_SCHEMA_1 = "SCHEMA1";
  private static final String MULTISCHEMA_SCHEMA_2 = "SCHEMA2";
  private static final String MULTISCHEMA_SCHEMA_3 = "THIRD_SCHEMA";

  private static Connection connection;
  private static TableJdbcELEvalContext tableJdbcELEvalContext;
  private static TableContextUtil tableContextUtil;

  @BeforeClass
  public static void setup() throws SQLException {
    tableContextUtil = UtilsProvider.getTableContextUtil();
    connection = DriverManager.getConnection(JDBC_URL, USER_NAME, PASSWORD);
    try (Statement s = connection.createStatement()) {
      Arrays.asList(SCHEMA, MULTISCHEMA_SCHEMA_1, MULTISCHEMA_SCHEMA_2, MULTISCHEMA_SCHEMA_3).forEach(schema -> {
        try {
          s.addBatch(String.format("CREATE SCHEMA IF NOT EXISTS %s;", schema));
          for (String tableName : TABLE_NAMES) {
            s.addBatch(String.format(CREATE_TABLE_PATTERN, schema, tableName));
          }
        } catch (SQLException e) {
          throw new RuntimeException("Failed to set up schemas for test", e);
        }
      });

      s.executeBatch();
    }
    Stage.Context context =
        ContextInfoCreator.createSourceContext(
            "a",
            false,
            OnRecordError.TO_ERROR,
            ImmutableList.of("a")
        );
    ELVars elVars = context.createELVars();
    TimeNowEL.setTimeNowInContext(elVars, new Date());
    tableJdbcELEvalContext = new TableJdbcELEvalContext(context, elVars);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Statement s = connection.createStatement()) {
      Arrays.asList(SCHEMA, MULTISCHEMA_SCHEMA_1, MULTISCHEMA_SCHEMA_2, MULTISCHEMA_SCHEMA_3).forEach(schema -> {
        try {
          for (String tableName : TABLE_NAMES) {
            s.addBatch(String.format(DELETE_TABLE_PATTERN, schema, tableName));
          }
          s.addBatch(String.format("DROP SCHEMA %s;", schema));
        } catch (SQLException e) {
          throw new RuntimeException("Failed to set up schemas for test", e);
        }
      });
      s.executeBatch();
    }
    connection.close();
    tableContextUtil = null;
  }

  public static Map<String, TableContext> listTablesForConfig(
      Connection connection,
      TableConfigBean tableConfigBean,
      TableJdbcELEvalContext tableJdbcELEvalContext
  ) throws SQLException, StageException {
    return tableContextUtil.listTablesForConfig(
        DatabaseVendor.UNKNOWN,
        createTestContext(),
        new LinkedList<Stage.ConfigIssue>(),
        connection,
        tableConfigBean,
        tableJdbcELEvalContext,
        QuoteChar.NONE
    );
  }

  @Test
  public void testNoExclusionPattern() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(SCHEMA)
        .build();
    Assert.assertEquals(
        TABLE_NAMES.size(),
        tableContextUtil.listTablesForConfig(
            DatabaseVendor.UNKNOWN,
            createTestContext(),
            new LinkedList<Stage.ConfigIssue>(),
            connection,
            tableConfigBean,
            tableJdbcELEvalContext,
            QuoteChar.NONE
        ).size()
    );
  }

  @Test
  public void testExcludeEverything() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(SCHEMA)
        .tableExclusionPattern(".*")
        .build();
    Assert.assertEquals(
        0,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  @Test
  public void testExcludeEndingWithNumbers() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TABLE%")
        .schema(SCHEMA)
        //Exclude tables ending with [0-9]+
        .tableExclusionPattern("TABLE[0-9]+")
        .build();
    Assert.assertEquals(
        5,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  @Test
  public void testExcludeTableNameAsRegex() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TABLE%")
        .schema(SCHEMA)
        .tableExclusionPattern("TABLE1")
        .build();

    Assert.assertEquals(
        9,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  @Test
  public void testExcludeUsingOrRegex() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TABLE%")
        .schema(SCHEMA)
        .tableExclusionPattern("TABLE1|TABLE2")
        .build();
    Assert.assertEquals(
        8,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  @Test
  public void testSchemaExclusion() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TABLE%")
        .schema("%")
        // exclude schemas SCHEMA1 and SCHEMA2, via a single character wildcard
        .schemaExclusionPattern("SCHEMA.")
        .build();
    Assert.assertEquals(
        // should include the test tables from TEST and THIRD_SCHEMA
        TABLE_NAMES.size() * 2,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  @Test
  public void testSchemaExclusionDisjunction() throws Exception {
    TableConfigBean tableConfigBean = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("TABLE%")
        .schema("%")
        // exclude schemas TEST and THIRD_SCHEMA
        .schemaExclusionPattern("TEST|THIRD.*")
        .build();
    Assert.assertEquals(
        // should include the test tables from TSCHEMA1 and SCHEMA2
        TABLE_NAMES.size() * 2,
        listTablesForConfig(connection, tableConfigBean, tableJdbcELEvalContext).size()
    );
  }

  private static PushSource.Context createTestContext() {
    return Mockito.mock(PushSource.Context.class);
  }
}
