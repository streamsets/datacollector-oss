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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Test;
import org.junit.Assert;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.SortedMap;

public class TestJdbcRecordReader {

  @Test
  public void testGetOperationFromRecordDefault(){
    // This record doesn't have sdc.operation.type, so default should be used
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    record.set(Field.create(fields));

    JdbcRecordReader reader = new JdbcRecordReader();
    List<OnRecordErrorException> errors = new ArrayList<>();

    try {
      Assert.assertEquals(
          OperationType.INSERT_CODE,
          reader.getOperationFromRecord(
              record,
              JDBCOperationType.INSERT,
              UnsupportedOperationAction.SEND_TO_ERROR,
              errors
          )
      );

      Assert.assertEquals(
          OperationType.DELETE_CODE,
          reader.getOperationFromRecord(
              record,
              JDBCOperationType.DELETE,
              UnsupportedOperationAction.SEND_TO_ERROR,
              errors
          )
      );

      Assert.assertEquals(
          OperationType.UPDATE_CODE,
          reader.getOperationFromRecord(
              record,
              JDBCOperationType.UPDATE,
              UnsupportedOperationAction.SEND_TO_ERROR,
              errors
          )
      );
    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertTrue(errors.isEmpty());
  }

  @Test
  public void testGetOperationFromRecordCodeInHeader() {
    JdbcRecordReader reader = new JdbcRecordReader();
    List<OnRecordErrorException> errors = new ArrayList<>();

    // This record contains proper sdd.operation.type in record header.
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    record.set(Field.create(fields));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.INSERT_CODE));

    Assert.assertEquals(
        OperationType.INSERT_CODE,
        reader.getOperationFromRecord(
            record,
            JDBCOperationType.DELETE, //default
            UnsupportedOperationAction.SEND_TO_ERROR,
            errors
        )
    );

    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.DELETE_CODE));
    Assert.assertEquals(
        OperationType.DELETE_CODE,
        reader.getOperationFromRecord(
            record,
            JDBCOperationType.INSERT, //default.
            UnsupportedOperationAction.SEND_TO_ERROR,
            errors
        )
    );

    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.UPDATE_CODE));
    Assert.assertEquals(
        OperationType.UPDATE_CODE,
        reader.getOperationFromRecord(
            record,
            JDBCOperationType.INSERT, //default.
            UnsupportedOperationAction.SEND_TO_ERROR,
            errors
        )
    );
    Assert.assertTrue(errors.isEmpty());
  }

  @Test
  public void testGetOperationFromRecordInvalidCodeInHeader() {
    // This record contains invalid value in sdd.operation.type in record header.
    // UnsupportedOperationAction should be applied.
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    record.set(Field.create(fields));
    // "INSERT" is invalid and causes NumberFormatException
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "INSERT");

    testUnsupportedOperationAction(record);
  }

  @Test
  public void testGetOperationFromRecordUnsupportedCodeInHeader() {
    // This record contains invalid value in sdd.operation.type in record header.
    // UnsupportedOperationAction should be applied.
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    record.set(Field.create(fields));
    // This will cause UnsupportedOperationException
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.UNSUPPORTED_CODE)); // this is invalid

    testUnsupportedOperationAction(record);
  }

  private void testUnsupportedOperationAction(Record record) {
    JdbcRecordReader reader = new JdbcRecordReader();
    List<OnRecordErrorException> errors = new ArrayList<>();

    // Test DISCARD
    try {
      Assert.assertEquals(
          -1,
          reader.getOperationFromRecord(
              record,
              JDBCOperationType.DELETE, //default
              UnsupportedOperationAction.DISCARD,
              errors
          )
      );
    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertTrue(errors.isEmpty());

    // Test USE_DEFAULT
    try {
      Assert.assertEquals(
          OperationType.DELETE_CODE,
          reader.getOperationFromRecord(
              record,
              JDBCOperationType.DELETE, //default
              UnsupportedOperationAction.USE_DEFAULT,
              errors
          )
      );
    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertTrue( errors.isEmpty());

    // Test SEND_TO_ERROR
    Assert.assertEquals(
        -1,
        reader.getOperationFromRecord(
            record,
            JDBCOperationType.DELETE, //default
            UnsupportedOperationAction.SEND_TO_ERROR,
            errors
        )
    );
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(JdbcErrors.JDBC_70, errors.get(0).getErrorCode());
  }

  @Test
  public void testGetColumnsToParameters(){
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("field1", Field.create("10"));
    fields.put("field2", Field.create("20"));
    record.set(Field.create(fields));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("col1", "?");
    parameters.put("col2", "CAST(? AS INTEGER)");
    parameters.put("col3", "?");

    Map<String, String> columnsToFields = new HashMap<>();
    columnsToFields.put("col1", "/field1");
    columnsToFields.put("col2", "/field2");
    columnsToFields.put("col3", "/field3");

    JdbcRecordReader reader = new JdbcRecordReader();
    SortedMap<String, String> res = reader.getColumnsToParameters(
        record,
        OperationType.INSERT_CODE,
        parameters,
        columnsToFields
    );
    Assert.assertEquals(2, res.size());
  }
}
