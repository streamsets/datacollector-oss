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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Ensure that we're creating tables in expected locations.
 */
@Ignore
public class LocationIT extends BaseHiveMetadataPropagationIT {

  private HiveMetastoreTarget hiveTarget;

  @Before
  public void setUp() {
    hiveTarget = new HiveMetastoreTargetBuilder()
      .build();
  }

  @Test
  public void testDefaultDatabase() throws Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database("default")
      .table("tbl")
      .partitions(Collections.emptyList())
      .build();

    processRecordsAndAssertData(processor,"default.tbl");
    assertTableLocation("default.tbl", "/user/hive/warehouse/tbl");
  }

  @Test
  public void testCustomDatabaseDefaultLocation() throws Exception {
    executeUpdate("CREATE DATABASE custom");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database("custom")
      .table("tbl")
      .partitions(Collections.emptyList())
      .build();

    processRecordsAndAssertData(processor,"custom.tbl");
    assertTableLocation("custom.tbl", "/user/hive/warehouse/custom.db/tbl");
  }

  @Test
  public void testCustomDatabaseCustomLocation() throws Exception {
    executeUpdate("CREATE DATABASE custom LOCATION '/user/hive/custom/'");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database("custom")
      .table("tbl")
      .partitions(Collections.emptyList())
      .build();

    processRecordsAndAssertData(processor,"custom.tbl");
    assertTableLocation("custom.tbl", "/user/hive/custom/tbl");
  }

  @Test
  public void testExternalTable() throws Exception {
    executeUpdate("CREATE DATABASE custom LOCATION '/user/hive/custom/'");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database("custom")
      .table("tbl")
      .external(true)
      .tablePathTemplate("/user/hive/tbl")
      .partitions(Collections.emptyList())
      .build();

    processRecordsAndAssertData(processor,"custom.tbl");
    assertTableLocation("custom.tbl", "/user/hive/tbl");
  }

  //@Test SDC-5459: Hive processor is ignoring location for internal tables
  public void testInternalTableWithCustomLocation() throws Exception {
    executeUpdate("CREATE DATABASE custom LOCATION '/user/hive/custom/'");
    executeUpdate("CREATE TABLE custom.tbl(id int) STORED AS AVRO LOCATION '/user/hive/tbl/'");

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database("custom")
      .table("tbl")
      .partitions(Collections.emptyList())
      .build();

    processRecordsAndAssertData(processor,"custom.tbl");
    assertTableLocation("custom.tbl", "/user/hive/tbl");
  }

  /**
   * Run the whole pipeline and validate that Hive can read the record back
   */
  public void processRecordsAndAssertData(HiveMetadataProcessor processor, String tableName) throws Exception{
    processRecords(processor, hiveTarget, getRecords());

    assertQueryResult(Utils.format("select * from {}", tableName), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.id", Types.INTEGER)
        );

        assertTrue("Table tbl doesn't contain any rows", rs.next());
        assertEquals(1, rs.getLong(1));
        assertFalse("Unexpected number of rows", rs.next());
      }
    });
  }

  /**
   * Create standard record that is used in all test methods of this test case
   */
  public List<Record> getRecords() {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("id", Field.create(Field.Type.INTEGER, 1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    return ImmutableList.of(record);
  }
}
