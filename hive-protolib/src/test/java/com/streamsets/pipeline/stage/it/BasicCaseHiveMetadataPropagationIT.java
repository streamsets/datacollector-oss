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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

public class BasicCaseHiveMetadataPropagationIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(BasicCaseHiveMetadataPropagationIT.class);

  HiveQueryExecutor hiveQueryExecutor;

  @Before
  public void setUpHiveQueryExecutor() {
    hiveQueryExecutor = new HiveQueryExecutor(getHiveJdbcUrl());
  }

  @Test
  public void testBasicIngestWhenNothingExists() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "StreamSets"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    processRecords(processor, hiveTarget, ImmutableList.of(record));

    Assert.assertTrue(hiveQueryExecutor.executeShowTableQuery("default.tbl"));
    try(
      Statement statement = getHiveConnection().createStatement();
      ResultSet rs = statement.executeQuery("select * from tbl");
    ) {
      ResultSetMetaData meta = rs.getMetaData();
      Assert.assertEquals(2, meta.getColumnCount());
      Assert.assertEquals("tbl.name", meta.getColumnName(1));
      Assert.assertEquals(Types.VARCHAR, meta.getColumnType(1));
      Assert.assertEquals("tbl.dt", meta.getColumnName(2));
      Assert.assertEquals(Types.VARCHAR, meta.getColumnType(2));

      Assert.assertTrue(rs.next());
      Assert.assertEquals("StreamSets", rs.getString(1));
      Assert.assertFalse(rs.next());
    }
  }
}
