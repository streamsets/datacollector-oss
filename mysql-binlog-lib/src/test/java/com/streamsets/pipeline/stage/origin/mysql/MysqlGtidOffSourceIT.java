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
package com.streamsets.pipeline.stage.origin.mysql;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

public class MysqlGtidOffSourceIT extends AbstractMysqlSource {
  @Rule
  public MySQLContainer mysql = new MySQLContainer("mysql:5.6").withConfigurationOverride("mysql_gtid_off");

  @Override
  public MySQLContainer createMysqlContainer() {
    return mysql;
  }

  @Test
  public void shouldWriteBinLogPosition() throws Exception {
    MysqlSourceConfig config = createConfig("root");
    MysqlSource source = createMysqlSource(config);
    SourceRunner runner = new SourceRunner.Builder(MysqlDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, MAX_BATCH_SIZE);
    List<Record> records = new ArrayList<>(output.getRecords().get(LANE));
    assertThat(records, is(Matchers.<Record>empty()));

    // add one more
    execute(ds, Arrays.asList(
        "INSERT INTO foo (bar) VALUES (2)",
        "INSERT INTO foo (bar) VALUES (3)")
    );
    output = runner.runProduce(null, MAX_BATCH_SIZE);
    records = new ArrayList<>(output.getRecords().get(LANE));
    assertThat(records, hasSize(2));

    assertThat(records.get(0).get("/BinLogFilename").getValueAsString(), is(notNullValue()));
    long position1 = records.get(0).get("/BinLogPosition").getValueAsLong();
    assertThat(records.get(1).get("/BinLogFilename").getValueAsString(), is(notNullValue()));
    long position2 = records.get(1).get("/BinLogPosition").getValueAsLong();
    assertThat(position2, is(greaterThan(position1)));
    assertThat(records.get(0).get("/Offset"), is(notNullValue()));

    assertThat(records.get(1).get("/Offset").getValueAsString(), is(output.getNewOffset()));
  }

  @Test
  public void shouldStartFromOffset() throws Exception {
    execute(ds, "INSERT INTO foo (bar) VALUES (1)");

    String offset = String.format("%s:%s", getBinlogFilename(), getBinlogPosition());

    execute(ds, "INSERT INTO foo (bar) VALUES (2)");

    MysqlSourceConfig config = createConfig("root");
    config.initialOffset = offset;
    MysqlSource source = createMysqlSource(config);
    SourceRunner runner = new SourceRunner.Builder(MysqlDSource.class, source)
        .addOutputLane(LANE)
        .build();
    runner.runInit();

    final String lastSourceOffset = null;
    StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
    List<Record> records = new ArrayList<>();

    while (!output.getRecords().get(LANE).isEmpty()) {
      records.addAll(output.getRecords().get(LANE));
      output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    }
    assertThat(records, hasSize(1));

    // add one more
    execute(ds, "INSERT INTO foo (bar) VALUES (3)");
    output = runner.runProduce(output.getNewOffset(), MAX_BATCH_SIZE);
    records.addAll(output.getRecords().get(LANE));

    assertThat(records, hasSize(2));

    for (Record record : records) {
      if (record.get("/Table").getValueAsString().equals("foo") &&
          record.get("/Data/bar").getValueAsInteger() == 1) {
        fail("Value before start offset found");
      }
    }
  }
}
