/**
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
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class TestTableJdbcSource {
  private static final String USER_NAME = "sa";
  private static final String PASSWORD = "sa";
  private static final String database = "TEST";
  private static final String JDBC_URL = "jdbc:h2:mem:" + database;

  private void testWrongConfiguration(TableJdbcSource tableJdbcSource, boolean isMockNeeded) throws Exception {
    if (isMockNeeded) {
      tableJdbcSource = Mockito.spy(tableJdbcSource);
      Mockito.doNothing().when(tableJdbcSource).checkConnectionAndBootstrap(
          Mockito.any(PushSource.Context.class), Mockito.anyListOf(Stage.ConfigIssue.class)
      );
    }
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testNoTableConfiguration() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(Collections.emptyList())
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testWrongSqlConnectionConfig() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder().tablePattern("testTable").build()
            )
        )
        .build();
    testWrongConfiguration(tableJdbcSource, false);
  }

  @Test
  public void testLessConnectionPoolSizeThanNoOfThreads() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder().tablePattern("testTable").build()
            )
        )
        .numberOfThreads(3)
        .maximumPoolSize(2)
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testWrongBatchesFromResultSetConfig() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
                    .tablePattern("testTable")
                    .build()
            )
        )
        .numberOfBatchesFromResultset(0)
        .batchTableStrategy(BatchTableStrategy.SWITCH_TABLES)
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }
}
