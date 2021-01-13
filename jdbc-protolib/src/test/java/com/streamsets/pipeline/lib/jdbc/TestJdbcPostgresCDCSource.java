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
package com.streamsets.pipeline.lib.jdbc;


import com.streamsets.pipeline.lib.jdbc.connection.PostgresConnection;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PgVersionValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCConfigBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TestJdbcPostgresCDCSource {

  private String username = "postgres";
  private String password = "postgres";

  private void createConfigBeans() {
    PostgresHikariPoolConfigBean hikariConfigBean = new PostgresHikariPoolConfigBean();
    hikariConfigBean.connection = new PostgresConnection();
    hikariConfigBean.connection.connectionString = "jdbc:postgresql://localhost:5432/sdctest";
    hikariConfigBean.connection.useCredentials = true;
    hikariConfigBean.connection.username = () -> username;
    hikariConfigBean.connection.password = () -> password;

    PostgresCDCConfigBean configBean = new PostgresCDCConfigBean();
    configBean.slot = "slot";
    configBean.minVersion = PgVersionValues.NINEFOUR;
    configBean.queryTimeout = 20;
    configBean.replicationType = "database";
    configBean.pollInterval = 1000;
    configBean.startLSN = "0/0";
  }

  @Before
  public void setup() {
    createConfigBeans();
  }

  @Test
  public void timeStampConv() {
    String mydate = "2018-07-06 12:57:33.383899-07";
    mydate += ":00";

    ZonedDateTime.parse(mydate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX"));
    Assert.assertTrue(true);

    mydate = "2018-07-09 10:16:23.815-07";
    mydate += ":00";
    ZonedDateTime.parse(mydate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX"));
    Assert.assertTrue(true);
  }

}
