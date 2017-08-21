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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableSortedMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Test scenario where input origin is not able to give us full schema of the source data and is rather sending
 * only a subset of the structure with each record (for example typically CDC).
 */
@Ignore
public class PartialInputIT extends  BaseHiveMetadataPropagationIT {

  @Test
  @SuppressWarnings("unchecked")
  public void testPartialInput() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .partitions(Collections.<PartitionConfig>emptyList())
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    List<Record> records = new LinkedList<>();
    Record record;

    // We insert each record twice to make sure that we can write more then one record to each rolled file

    record = RecordCreator.create("s", "s:0");
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 0)
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 1)
    )));
    records.add(record);

    record = RecordCreator.create("s", "s:2");
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 2),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:3");
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 3),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);

    record = RecordCreator.create("s", "s:4");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 4),
      "name", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:5");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 5),
      "name", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);

    record = RecordCreator.create("s", "s:6");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 6),
      "value", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:7");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 7),
      "value", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);

    record = RecordCreator.create("s", "s:8");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 8),
      "value", Field.create(Field.Type.STRING, "text"),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:9");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 9),
      "value", Field.create(Field.Type.STRING, "text"),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);

    record = RecordCreator.create("s", "s:10");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 10),
      "name", Field.create(Field.Type.STRING, "text"),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);
    record = RecordCreator.create("s", "s:11");
    record.set(Field.create(Field.Type.MAP, ImmutableSortedMap.of(
      "index", Field.create(Field.Type.INTEGER, 11),
      "name", Field.create(Field.Type.STRING, "text"),
      "id", Field.create(Field.Type.STRING, "text")
    )));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    // End state should be with three columns
    assertTableStructure("default.tbl",
      ImmutablePair.of("tbl.index", Types.INTEGER),
      ImmutablePair.of("tbl.id", Types.VARCHAR),
      ImmutablePair.of("tbl.name", Types.VARCHAR),
      ImmutablePair.of("tbl.value", Types.VARCHAR)
    );

    assertQueryResult("select * from tbl order by index", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertNull(rs.getString("id"));
          Assert.assertNull(rs.getString("name"));
          Assert.assertNull(rs.getString("value"));
        }

        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals("text", rs.getString("id"));
          Assert.assertNull(rs.getString("name"));
          Assert.assertNull(rs.getString("value"));
        }

        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertNull(rs.getString("id"));
          Assert.assertEquals("text", rs.getString("name"));
          Assert.assertNull(rs.getString("value"));
        }

        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertNull(rs.getString("id"));
          Assert.assertNull(rs.getString("name"));
          Assert.assertEquals("text", rs.getString("value"));
        }

        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals("text", rs.getString("id"));
          Assert.assertNull(rs.getString("name"));
          Assert.assertEquals("text", rs.getString("value"));
        }

        for(int i = 0; i < 2; i++) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals("text", rs.getString("id"));
          Assert.assertEquals("text", rs.getString("name"));
          Assert.assertNull(rs.getString("value"));
        }

        Assert.assertFalse(rs.next());
      }
    });

    Assert.assertEquals(4, getDefaultFileSystem().listStatus(new Path("/user/hive/warehouse/tbl/")).length);
  }

}
