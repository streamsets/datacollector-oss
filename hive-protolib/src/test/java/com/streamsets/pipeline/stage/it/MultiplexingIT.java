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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *  Validate stream of multiple tables with different structures.
 */
@Ignore
public class MultiplexingIT extends BaseHiveMetadataPropagationIT {
  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexing() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .table("${record:attribute('table')}")
      .partitions(new PartitionConfigBuilder()
        .addPartition("country", HiveType.STRING, "${record:attribute('country')}")
        .addPartition("year", HiveType.STRING, "${record:attribute('year')}")
        .build()
      )
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    // We build stream that have two tables
    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("name", Field.create(Field.Type.STRING, "San Francisco"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("table", "towns");
    record.getHeader().setAttribute("country", "US");
    record.getHeader().setAttribute("year", "2016");
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("customer", Field.create(Field.Type.STRING, "Santhosh"));
    map.put("value", Field.create(Field.Type.INTEGER, 200));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("table", "invoice");
    record.getHeader().setAttribute("country", "India");
    record.getHeader().setAttribute("year", "2015");
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("name", Field.create(Field.Type.STRING, "Brno"));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("table", "towns");
    record.getHeader().setAttribute("country", "CR");
    record.getHeader().setAttribute("year", "2016");
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("customer", Field.create(Field.Type.STRING, "Junko"));
    map.put("value", Field.create(Field.Type.INTEGER, 300));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("table", "invoice");
    record.getHeader().setAttribute("country", "Japan");
    record.getHeader().setAttribute("year", "2015");
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertTableExists("default.towns");
    assertTableExists("default.invoice");
    assertQueryResult("select * from towns order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
          ImmutablePair.of("towns.id", Types.INTEGER),
          ImmutablePair.of("towns.name", Types.VARCHAR),
          ImmutablePair.of("towns.country", Types.VARCHAR),
          ImmutablePair.of("towns.year", Types.VARCHAR)
        );

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("San Francisco", rs.getString(2));
        Assert.assertEquals("US", rs.getString(3));
        Assert.assertEquals("2016", rs.getString(4));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("Brno", rs.getString(2));
        Assert.assertEquals("CR", rs.getString(3));
        Assert.assertEquals("2016", rs.getString(4));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
    assertQueryResult("select * from invoice order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
          ImmutablePair.of("invoice.id", Types.INTEGER),
          ImmutablePair.of("invoice.customer", Types.VARCHAR),
          ImmutablePair.of("invoice.value", Types.INTEGER),
          ImmutablePair.of("invoice.country", Types.VARCHAR),
          ImmutablePair.of("invoice.year", Types.VARCHAR)
        );

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals("Santhosh", rs.getString(2));
        Assert.assertEquals(200, rs.getLong(3));
        Assert.assertEquals("India", rs.getString(4));
        Assert.assertEquals("2015", rs.getString(5));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals("Junko", rs.getString(2));
        Assert.assertEquals(300, rs.getLong(3));
        Assert.assertEquals("Japan", rs.getString(4));
        Assert.assertEquals("2015", rs.getString(5));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }
}
