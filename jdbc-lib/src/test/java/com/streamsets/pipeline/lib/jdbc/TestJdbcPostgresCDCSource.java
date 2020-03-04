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


import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.DecoderValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PgVersionValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCSource;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresWalRecord;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresCDCWalReceiver;
import com.zaxxer.hikari.HikariDataSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.powermock.api.mockito.PowerMockito;

public class TestJdbcPostgresCDCSource {

  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private PostgresCDCConfigBean configBean;
  private HikariPoolConfigBean hikariConfigBean;
  private String h2ConnectionString = "jdbc:postgresql://localhost:5432/sdctest";
  private String username = "postgres";
  private String password = "postgres";
    private PostgresCDCWalReceiver walReceiver;

  private void createConfigBeans() {
    hikariConfigBean = new HikariPoolConfigBean();
    hikariConfigBean.connectionString = h2ConnectionString;
    hikariConfigBean.useCredentials = true;
    hikariConfigBean.username = () -> username;
    hikariConfigBean.password = () -> password;

    configBean = new PostgresCDCConfigBean();
    configBean.slot = "slot";
    configBean.minVersion = PgVersionValues.NINEFOUR;
    configBean.queryTimeout = 20;
    configBean.replicationType = "database";
    configBean.pollInterval = 1000;
    configBean.startLSN = "0/0";

  }

  private Source.Context context;
  private Source.Context contextInPreview;


  @Before
  public void setup() {
    context = (Source.Context) ContextInfoCreator
        .createSourceContext("s", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    contextInPreview = (Source.Context) ContextInfoCreator.createSourceContext("s", true, OnRecordError.TO_ERROR,
        ImmutableList.of("a"));
    createConfigBeans();

  }

  @Test
  public void timeStampConv()
  {
    String mydate = "2018-07-06 12:57:33.383899-07";
    if(mydate.length() == 29) {
      mydate += ":00";
    }

    ZonedDateTime zonedDateTime = ZonedDateTime
        .parse(mydate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX"));
    Assert.assertTrue(true);

    mydate = "2018-07-09 10:16:23.815-07";
    if(mydate.length() == 26) {
      mydate += ":00";
      zonedDateTime = ZonedDateTime
          .parse(mydate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX"));
      Assert.assertTrue(true);
    }
  }

}
