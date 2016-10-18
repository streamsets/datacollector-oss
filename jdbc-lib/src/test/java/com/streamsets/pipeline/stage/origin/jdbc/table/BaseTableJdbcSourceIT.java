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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class BaseTableJdbcSourceIT {
  protected static final String USER_NAME = "sa";
  protected static final String PASSWORD = "sa";
  protected static final String database = "TEST";
  protected static final String JDBC_URL = "jdbc:h2:mem:" + database;
  protected static Connection connection;

  @BeforeClass
  public static void setup() throws SQLException {
    connection = DriverManager.getConnection(JDBC_URL, USER_NAME, PASSWORD);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  private static void checkField(String fieldPath, Field expectedField, Field actualField) throws Exception {
    String errorString = String.format("Error in Field Path: %s", fieldPath);
    Assert.assertEquals(errorString, expectedField.getType(), actualField.getType());
    errorString = errorString + " of type: " + expectedField.getType().name();
    switch (expectedField.getType()) {
      case MAP:
      case LIST_MAP:
        Map<String, Field> expectedFieldMap = expectedField.getValueAsMap();
        Map<String, Field> actualFieldMap = actualField.getValueAsMap();
        Assert.assertEquals(errorString, expectedFieldMap.keySet().size(), actualFieldMap.keySet().size());
        for (String key : actualFieldMap.keySet()) {
          Assert.assertNotNull(errorString, expectedFieldMap.get(key.toLowerCase()));
          checkField(fieldPath + "/" + key.toLowerCase(), expectedFieldMap.get(key.toLowerCase()), actualFieldMap.get(key));
        }
        break;
      case LIST:
        List<Field> expectedFieldList = expectedField.getValueAsList();
        List<Field> actualFieldList = actualField.getValueAsList();
        Assert.assertEquals(errorString, expectedFieldList.size(), actualFieldList.size());
        for (int i = 0; i < expectedFieldList.size(); i++) {
          checkField(fieldPath + "[" + i + "]", expectedFieldList.get(i), actualFieldList.get(i));
        }
        break;
      case BYTE_ARRAY:
        Assert.assertArrayEquals(errorString, expectedField.getValueAsByteArray(), actualField.getValueAsByteArray());
        break;
      default:
        Assert.assertEquals(errorString, expectedField.getValue(), actualField.getValue());
    }
  }

  static void checkRecords(List<Record> expectedRecords, List<Record> actualRecords) throws Exception {
    Assert.assertEquals("Record Size Does not match.", expectedRecords.size(), actualRecords.size());
    for (int i = 0; i < actualRecords.size(); i++) {
      Record actualRecord = actualRecords.get(i);
      Record expectedRecord = expectedRecords.get(i);
      checkField("", expectedRecord.get(), actualRecord.get());
    }
  }
}
