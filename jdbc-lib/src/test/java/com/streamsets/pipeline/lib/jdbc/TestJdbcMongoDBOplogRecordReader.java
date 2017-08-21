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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestJdbcMongoDBOplogRecordReader {

  @Test
  public void testGetColumnsToParametersInsertRecord() {
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    Map<String, Field> o = new HashMap<>();
    o.put("_id", Field.create("5875d72a16cfd5cfcd99d0cb"));
    o.put("name", Field.create("streamsets"));
    o.put("city", Field.create("San Francisco"));
    fields.put("o", Field.create(o));
    record.set(Field.create(fields));
    // column name to parameter mapping
    Map<String, String> parameter = ImmutableMap.of(
        "id", "?",
        "name", "?",
        "city", "?"
    );
    // column name to field name mapping
    Map<String, String> columnsToFields = ImmutableMap.of(
      "id", "/o/_id",
      "name", "/o/name",
      "city", "/o/city"
    );
    // expected result
    Map<String, String> expected = new HashMap<>(parameter);
    JdbcRecordReader reader = new JdbcMongoDBOplogRecordReader();
    // record has all fields defined in column-to-field mapping
    Assert.assertEquals(
        expected,
        reader.getColumnsToParameters(record,
        OperationType.INSERT_CODE, parameter, columnsToFields));

    // Record is missing one field
    Record r2 = RecordCreator.create();
    Map<String, Field> field2 = new HashMap<>();
    Map<String, Field> o2 = new HashMap<>();
    o2.put("_id", Field.create("5875d72a16cfd5cfcd99d0cb"));
    o2.put("name", Field.create("streamsets"));
    field2.put("o", Field.create(o2));
    r2.set(Field.create(field2));

    expected.remove("city");
    Assert.assertEquals(
        expected,
        reader.getColumnsToParameters(r2,
            OperationType.INSERT_CODE, parameter, columnsToFields));

  }

  @Test
  public void testGetColumnsToParametersDeleteRecord() {
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    Map<String, Field> o = new HashMap<>();
    o.put("_id", Field.create("5875d72a16cfd5cfcd99d0cb"));
    fields.put("o", Field.create(o));
    record.set(Field.create(fields));
    // column name to parameter mapping
    Map<String, String> parameter = ImmutableMap.of(
        "id", "?",
        "name", "?",
        "city", "?"
    );
    // column name to field name mapping
    Map<String, String> columnsToFields = ImmutableMap.of(
        "id", "/o/_id",
        "name", "/o/name",
        "city", "/o/city"
    );
    // expected result
    Map<String, String> expected = ImmutableMap.of(
      "id", "?"
    );

    JdbcRecordReader reader = new JdbcMongoDBOplogRecordReader();
    // record has all fields defined in column-to-field mapping
    Assert.assertEquals(
        expected,
        reader.getColumnsToParameters(record,
            OperationType.DELETE_CODE, parameter, columnsToFields));

  }

  @Test
  public void testGetColumnsToParametersUpdateRecord() {
    /* Structure of update record
      /o2 (map)
        /_id -> objectId
      /o (map)
        /$set (map)
           /col1  -> "xxx"  //updating col and value
    */
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    Map<String, Field> o2 = ImmutableMap.of(
      "_id", Field.create("5875d72a16cfd5cfcd99d0cb")
    );
    fields.put("o2", Field.create(o2));

    Map<String, Field> set = new HashMap<>();
    set.put("name", Field.create("streamsets"));
    set.put("city", Field.create("San Francisco"));
    Map<String, Field> o = ImmutableMap.of(
        "$set", Field.create(set)
    );
    fields.put("o", Field.create(o));
    record.set(Field.create(fields));

    // column name to parameter mapping
    Map<String, String> parameter = ImmutableMap.of(
        "id", "?",
        "name", "?",
        "city", "?"
    );
    // column name to field name mapping
    Map<String, String> columnsToFields = ImmutableMap.of(
        "id", "/o/_id",
        "name", "/o/name",
        "city", "/o/city"
    );
    // expected result
    Map<String, String> expected = new HashMap<>(parameter);

    JdbcRecordReader reader = new JdbcMongoDBOplogRecordReader();
    // record has all fields defined in column-to-field mapping
    Assert.assertEquals(
        expected,
        reader.getColumnsToParameters(record,
            OperationType.UPDATE_CODE, parameter, columnsToFields));

  }

  @Test
  public void testGetFieldPath(){
    JdbcRecordReader reader = new JdbcMongoDBOplogRecordReader();

    Map<String, String> columnsToFields = ImmutableMap.of(
        "id", "/o/_id",
        "name", "/o/name",
        "city", "/o/city",
        "owner", "/o/owner"
    );
    Assert.assertEquals(
        "/o/_id",
        reader.getFieldPath("id", columnsToFields, OperationType.INSERT_CODE)
    );
    Assert.assertEquals(
        "/o/name",
        reader.getFieldPath("name", columnsToFields, OperationType.INSERT_CODE)
    );

    Assert.assertEquals(
        "/o/_id",
        reader.getFieldPath("id", columnsToFields, OperationType.DELETE_CODE)
    );

    Assert.assertEquals(
        "/o2/_id",
        reader.getFieldPath("id", columnsToFields, OperationType.UPDATE_CODE)
    );
    Assert.assertEquals(
        "/o/$set/name",
        reader.getFieldPath("name", columnsToFields, OperationType.UPDATE_CODE)
    );

    Assert.assertEquals(
        "/o/$set/owner",
        reader.getFieldPath("owner", columnsToFields, OperationType.UPDATE_CODE)
    );
  }

}
