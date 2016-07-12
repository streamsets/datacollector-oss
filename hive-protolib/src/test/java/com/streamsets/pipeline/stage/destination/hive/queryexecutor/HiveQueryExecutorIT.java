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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * To verify that the Hive query is indeed executed properly, we're creating tables and validating their proper structure.
 */
public class HiveQueryExecutorIT extends BaseHiveIT {

  @Before
  public void createBaseTable() throws Exception {
    executeUpdate("CREATE TABLE `origin` (id int, name string)");
  }

  @Test
  public void testExecuteSimpleQuery() throws Exception {
    HiveQueryExecutor queryExecutor = createExecutor("CREATE TABLE copy AS SELECT * FROM origin");

    TargetRunner runner = new TargetRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create("blank line"));

    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertTableStructure("default.copy",
      new ImmutablePair("copy.id", Types.INTEGER),
      new ImmutablePair("copy.name", Types.VARCHAR)
    );
  }

  @Test
  public void testEL() throws Exception {
    HiveQueryExecutor queryExecutor = createExecutor("CREATE TABLE ${record:value('/table')} AS SELECT * FROM origin");

    TargetRunner runner = new TargetRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    Map<String, Field> map = new HashMap<>();
    map.put("table", Field.create("el"));

    Record record = RecordCreator.create();
    record.set(Field.create(map));

    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    assertTableStructure("default.el",
      new ImmutablePair("el.id", Types.INTEGER),
      new ImmutablePair("el.name", Types.VARCHAR)
    );
  }

  @Test
  public void testIncorrectQuery() throws Exception {
    HiveQueryExecutor queryExecutor = createExecutor("THIS REALLY IS NOT VALID QUERY");

    TargetRunner runner = new TargetRunner.Builder(HiveQueryDExecutor.class, queryExecutor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create("FIELD"));

    runner.runWrite(ImmutableList.of(record));
    List<Record> errors = runner.getErrorRecords();
    assertNotNull(errors);
    assertEquals(1, errors.size());
    assertEquals("FIELD", errors.get(0).get().getValueAsString());

    runner.runDestroy();
  }

  public HiveQueryExecutor createExecutor(String query) {
    HiveQueryExecutorConfig config = new HiveQueryExecutorConfig();
    config.hiveConfigBean = BaseHiveIT.getHiveConfigBean();
    config.query = query;
    return new HiveQueryExecutor(config);
  }
}
