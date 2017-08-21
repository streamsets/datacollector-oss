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
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Since decimal is fairly advanced type that required a lot of change, we're testing it explicitly here
 */
@Ignore
public class DecimalTypeIT extends BaseHiveMetadataPropagationIT {

  @Test
  @SuppressWarnings("unchecked")
  public void correctCases() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int, dec decimal(4, 2)) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .decimalConfig(4, 2)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("dec", Field.create(BigDecimal.valueOf(12.12)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 2));
    map.put("dec", Field.create(BigDecimal.valueOf(1.0)));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 3));
    map.put("dec", Field.create(BigDecimal.valueOf(12.0)));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 4));
    map.put("dec", Field.create(BigDecimal.valueOf(0.1)));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 5));
    map.put("dec", Field.create(BigDecimal.valueOf(0.12)));
    record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.dec", Types.DECIMAL),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(12.12), rs.getBigDecimal(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(2, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(1), rs.getBigDecimal(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(3, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(12), rs.getBigDecimal(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(4, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(0.1), rs.getBigDecimal(2));

        Assert.assertTrue("Unexpected number of rows", rs.next());
        Assert.assertEquals(5, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(0.12), rs.getBigDecimal(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  @Test
  public void incompatibleScale() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int, dec decimal(4, 2)) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .decimalConfig(4, 2)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("dec", Field.create(BigDecimal.valueOf(0.123)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Expected failure!");
    } catch(OnRecordErrorException e) {
      Assert.assertEquals(Errors.HIVE_26, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("scale 3 is more then expected 2"));
    }
  }

  @Test
  public void incompatiblePrecision() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int, dec decimal(4, 2)) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .decimalConfig(4, 2)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("dec", Field.create(BigDecimal.valueOf(12345)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    try {
      processRecords(processor, hiveTarget, records);
      Assert.fail("Expected failure!");
    } catch(OnRecordErrorException e) {
      Assert.assertEquals(Errors.HIVE_26, e.getErrorCode());
      Assert.assertTrue(e.getMessage().contains("precision 5 is more then expected 4"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void zeroScale() throws Exception {
    executeUpdate("CREATE TABLE `tbl` (id int, dec decimal(2, 0)) PARTITIONED BY (dt string) STORED AS AVRO");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .decimalConfig(2, 0)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    List<Record> records = new LinkedList<>();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    map.put("dec", Field.create(BigDecimal.valueOf(12)));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    records.add(record);

    processRecords(processor, hiveTarget, records);

    assertQueryResult("select * from tbl order by id", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER),
            ImmutablePair.of("tbl.dec", Types.DECIMAL),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(BigDecimal.valueOf(12), rs.getBigDecimal(2));

        Assert.assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

}
