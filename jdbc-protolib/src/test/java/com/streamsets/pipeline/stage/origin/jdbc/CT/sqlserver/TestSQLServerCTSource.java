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
package com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.sdk.PushSourceRunner;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestSQLServerCTSource {
    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "sa";
    private static final String database = "TEST";
    private static final String JDBC_URL = "jdbc:sqlserver://localhost:1433" + database;

    private void testWrongConfiguration(SQLServerCTSource tableJdbcSource) throws Exception {
      PushSourceRunner runner = new PushSourceRunner.Builder(SQLServerCTDSource.class, tableJdbcSource)
          .addOutputLane("a").build();
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Assert.assertEquals(1, issues.size());
    }

    @Test
    public void testNoTableConfiguration() throws Exception {
      SQLServerCTSource tableJdbcSource = new SQLServerCTSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(Collections.emptyList())
          .build();
      testWrongConfiguration(tableJdbcSource);
    }

    @Test
    public void testWrongSqlConnectionConfig() throws Exception {
      CTTableConfigBean tableConfigBean = new CTTableConfigBean();
      tableConfigBean.schema = "dbo";
      tableConfigBean.tablePattern = "testTable";
      SQLServerCTSource tableJdbcSource = new SQLServerCTSourceTestBuilder( JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(
              ImmutableList.of(tableConfigBean
              )
          )
          .build();
      testWrongConfiguration(tableJdbcSource);
    }

    @Test
    public void testLessConnectionPoolSizeThanNoOfThreads() throws Exception {
      CTTableConfigBean tableConfigBean = new CTTableConfigBean();
      tableConfigBean.schema = "dbo";
      tableConfigBean.tablePattern = "testTable";
      SQLServerCTSource tableJdbcSource = new SQLServerCTSourceTestBuilder( JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(
              ImmutableList.of(
                  tableConfigBean
              )
          )
          .numberOfThreads(3)
          .maximumPoolSize(2)
          .build();
      testWrongConfiguration(tableJdbcSource);
    }

    @Test
    public void testWrongBatchesFromResultSetConfig() throws Exception {
      CTTableConfigBean tableConfigBean = new CTTableConfigBean();
      tableConfigBean.schema = "dbo";
      tableConfigBean.tablePattern = "testTable";
      SQLServerCTSource tableJdbcSource = new SQLServerCTSourceTestBuilder( JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(
              ImmutableList.of(
                  tableConfigBean
              )
          )
          .numberOfBatchesFromResultset(0)
          .batchTableStrategy(BatchTableStrategy.SWITCH_TABLES)
          .build();
      testWrongConfiguration(tableJdbcSource);
    }

    @Test
    public void testHandleLastOffsetWithNull() {
      CTTableConfigBean tableConfigBean = new CTTableConfigBean();
      SQLServerCTSource tableJdbcSource = new SQLServerCTSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(
              ImmutableList.of(
                  tableConfigBean
              )
          )
          .numberOfThreads(3)
          .maximumPoolSize(2)
          .build();
      // Sholdn't throw NPE
      tableJdbcSource.handleLastOffset(null);
    }
  }
