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

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class TestJdbcMicrosoftRecordReader {
  @Test
  public void testGetOperationFromRecordNoOperationInHeader() throws Exception {
    Record record = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    // No operatin specified in header, and default is insert.
    record.set(Field.create(insert));

    List< OnRecordErrorException > errorRecords = new ArrayList<>();

    JdbcRecordReader msReader = new JdbcMicrosoftRecordReader();
    assertEquals(
        OperationType.INSERT_CODE,
        msReader.getOperationFromRecord(record, JDBCOperationType.INSERT, UnsupportedOperationAction.DISCARD, errorRecords)
    );

    assertEquals(
        OperationType.UPDATE_CODE,
        msReader.getOperationFromRecord(record, JDBCOperationType.UPDATE, UnsupportedOperationAction.DISCARD, errorRecords)
    );

    assertEquals(
        OperationType.DELETE_CODE,
        msReader.getOperationFromRecord(record, JDBCOperationType.DELETE, UnsupportedOperationAction.DISCARD, errorRecords)
    );
    assertEquals(0, errorRecords.size());
  }

  @Test
  public void testGetOperationFromRecordWithOperationCodeInHeader() throws Exception {
    Record record = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    record.set(Field.create(insert));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.INSERT_CODE));

    List< OnRecordErrorException > errorRecords = new ArrayList<>();

    JdbcRecordReader msReader = new JdbcMicrosoftRecordReader();
    assertEquals(
        OperationType.INSERT_CODE,
        msReader.getOperationFromRecord(record, JDBCOperationType.DELETE, UnsupportedOperationAction.DISCARD, errorRecords)
    );

    Record record2 = RecordCreator.create();
    record2.set(Field.create(insert));
    record2.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.UPDATE_CODE));
    assertEquals(
        OperationType.UPDATE_CODE,
        msReader.getOperationFromRecord(record2, JDBCOperationType.DELETE, UnsupportedOperationAction.DISCARD, errorRecords)
    );

    Record record3 = RecordCreator.create();
    record3.set(Field.create(insert));
    record3.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.DELETE_CODE));
    assertEquals(
        OperationType.DELETE_CODE,
        msReader.getOperationFromRecord(record3, JDBCOperationType.INSERT, UnsupportedOperationAction.DISCARD, errorRecords)
    );
    assertEquals(0, errorRecords.size());
  }

  @Test
  public void testGetOperationFromRecordUnsupportedOperation() throws Exception {
    JdbcRecordReader msReader = new JdbcMicrosoftRecordReader();

    Record record = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    record.set(Field.create(insert));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "wrong code");

    List< OnRecordErrorException > errorRecords = new ArrayList<>();

    assertEquals(
        OperationType.INSERT_CODE,
        msReader.getOperationFromRecord(record, JDBCOperationType.INSERT, UnsupportedOperationAction.USE_DEFAULT, errorRecords)
    );

    Record record2 = RecordCreator.create();
    record2.set(Field.create(insert));
    record2.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(OperationType.UNSUPPORTED_CODE));
    assertEquals(
        OperationType.DELETE_CODE,
        msReader.getOperationFromRecord(record2, JDBCOperationType.DELETE, UnsupportedOperationAction.USE_DEFAULT, errorRecords)
    );

    assertEquals(
        -1,
        msReader.getOperationFromRecord(record2, JDBCOperationType.UPDATE, UnsupportedOperationAction.DISCARD, errorRecords)
    );
    assertEquals(0, errorRecords.size());
  }

  @Test
  public void testGetOperationFromRecordSendToError() throws Exception {
    JdbcRecordReader msReader = new JdbcMicrosoftRecordReader();

    Record record = RecordCreator.create();
    Map<String, Field> insert = new HashMap<>();
    insert.put("P_ID", Field.create(300));
    record.set(Field.create(insert));
    record.getHeader().setAttribute(OperationType.SDC_OPERATION_TYPE, "INSERT"); // this is wrong

    List< OnRecordErrorException > errorRecords = new ArrayList<>();

    assertEquals(
        -1,
        msReader.getOperationFromRecord(record, JDBCOperationType.INSERT, UnsupportedOperationAction.SEND_TO_ERROR, errorRecords)
    );
    assertEquals(1, errorRecords.size());
    assertEquals(JdbcErrors.JDBC_70, errorRecords.get(0).getErrorCode());
    assertEquals(record, errorRecords.get(0).getRecord());
  }

}
