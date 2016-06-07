/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.it;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Validates what happens on each drift type (columns added, removed, changed).
 */
public class DriftIT extends  BaseHiveMetadataPropagationIT {

  @Before
  public void createTestTable() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int) PARTITIONED BY (dt string) STORED AS AVRO");
  }

  @Test
  public void testNewColumn() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("new_column", Field.create(Field.Type.STRING, "new value"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
          new ImmutablePair("tbl.id", Types.INTEGER),
          new ImmutablePair("tbl.new_column", Types.VARCHAR),
          new ImmutablePair("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(null, rs.getString(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("new value", rs.getString(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

//  @Test SDC-3127
  public void testRemovedColumn() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("removed", Field.create(Field.Type.STRING, "value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
          new ImmutablePair("tbl.id", Types.INTEGER),
          new ImmutablePair("tbl.removed", Types.VARCHAR),
          new ImmutablePair("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("value", rs.getString(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals(null, rs.getString(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void testChangedColumnType() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.STRING, "2"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Column type change haven't resulted in exception");
    } catch (StageException e) {
      Assert.assertEquals(Errors.HIVE_21, e.getErrorCode());
    }
  }

}
