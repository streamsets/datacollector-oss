/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class TestTableJdbcSource {
  static HikariPoolConfigBean createHikariPoolConfigBean(String connectionString, String username, String password) {
    HikariPoolConfigBean hikariPoolConfigBean = new HikariPoolConfigBean();
    hikariPoolConfigBean.connectionString = connectionString;
    hikariPoolConfigBean.username = username;
    hikariPoolConfigBean.password = password;
    return hikariPoolConfigBean;
  }

  static CommonSourceConfigBean createCommonSourceConfigBean(long queryInterval, int maxBatchSize, int maxClobSize, int maxBlobSize) {
    return new CommonSourceConfigBean(queryInterval, maxBatchSize, maxClobSize, maxBlobSize);
  }

  static TableJdbcConfigBean createPartitionableConfigBean(List<TableConfigBean> tableConfigs, boolean configureFetchSize, int fetchSize) {
    TableJdbcConfigBean tableJdbcConfigBean = new TableJdbcConfigBean();
    tableJdbcConfigBean.tableConfigs = tableConfigs;
    tableJdbcConfigBean.configureFetchSize = configureFetchSize;
    tableJdbcConfigBean.fetchSize = fetchSize;
    return tableJdbcConfigBean;
  }

  private void testWrongConfiguration(TableJdbcSource tableJdbcSource, boolean isMockNeeded) throws Exception {
    if (isMockNeeded) {
      tableJdbcSource = Mockito.spy(tableJdbcSource);
      Mockito.doNothing().when(tableJdbcSource).checkConnectionAndBootstrap(
          Mockito.any(Source.Context.class), Mockito.anyListOf(Stage.ConfigIssue.class)
      );
    }
    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testNoTableConfiguration() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        createHikariPoolConfigBean("jdbc:h2:mem:database", "sa", "test"),
        createCommonSourceConfigBean(1000, 1000, 1000, 1000),
        createPartitionableConfigBean(Collections.<TableConfigBean>emptyList(), false, -1)
    );

    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testFetchSizeGreaterThanBatchSize() throws Exception {
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "testTable";

    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        createHikariPoolConfigBean("jdbc:h2:mem:database", "sa", "test"),
        createCommonSourceConfigBean(1000, 1000, 1000, 1000),
        createPartitionableConfigBean(ImmutableList.of(tableConfigBean), true, 2000)
    );
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testWrongSqlConnectionConfig() throws Exception {
    TableConfigBean tableConfigBean = new TableConfigBean();
    tableConfigBean.tablePattern = "testTable";
    TableJdbcSource tableJdbcSource = new TableJdbcSource(
        createHikariPoolConfigBean("jdbc:db://localhost:1000", "sa", "test"),
        createCommonSourceConfigBean(1000, 1000, 1000, 1000),
        createPartitionableConfigBean(ImmutableList.of(tableConfigBean), false, 1)
    );
    testWrongConfiguration(tableJdbcSource, false);
  }
}
