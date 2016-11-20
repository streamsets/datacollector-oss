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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJdbcUtil {

  private final String username = "sa";
  private final String password = "sa";
  private final String database = "test";
  private final String h2ConnectionString = "jdbc:h2:mem:" + database;

  private HikariPoolConfigBean createConfigBean() {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = h2ConnectionString;
    bean.username = username;
    bean.password = password;

    return bean;
  }

  @Test
  public void testTransactionIsolation() throws Exception {
    HikariPoolConfigBean config = createConfigBean();
    config.transactionIsolation = TransactionIsolationLevel.TRANSACTION_READ_COMMITTED;

    HikariDataSource dataSource = JdbcUtil.createDataSourceForRead(config, new Properties());
    Connection connection = dataSource.getConnection();
    assertNotNull(connection);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
  }

}
