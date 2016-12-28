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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ReferentialConstraintOrderingIT extends BaseTableJdbcSourceIT {

  //
  //                  -- ORDER_TBL <--
  //                 |                |
  //                \|/               |
  //                USER            ITEMS
  //                                  |
  //                                 \|/
  //                                PRODUCT

  private static final String USER_INSERT_TEMPLATE  = "INSERT INTO TEST.USER VALUES (%s, '%s', '%s');";
  private static final String PRODUCT_INSERT_TEMPLATE  = "INSERT INTO TEST.PRODUCT VALUES (%s, '%s', '%s');";
  private static final String ORDER_TBL_INSERT_TEMPLATE  = "INSERT INTO TEST.ORDER_TBL VALUES (%s, %s);";
  private static final String ITEMS_INSERT_TEMPLATE = "INSERT INTO TEST.ITEMS VALUES (%s, %s, %s, %s);";

  private static final Map<String, Pair<String, ArrayList<Record>>> TABLE_TO_TEMPLATE_AND_RECORDS_MAP =
      new ImmutableMap.Builder<String, Pair<String, ArrayList<Record>>>()
          .put("USER", Pair.of(USER_INSERT_TEMPLATE, new ArrayList<Record>()))
          .put("PRODUCT", Pair.of(PRODUCT_INSERT_TEMPLATE, new ArrayList<Record>()))
          .put("ORDER_TBL", Pair.of(ORDER_TBL_INSERT_TEMPLATE, new ArrayList<Record>()))
          .put("ITEMS", Pair.of(ITEMS_INSERT_TEMPLATE, new ArrayList<Record>()))
          .build();

  private static void populateRecords() {
    Record record ;
    LinkedHashMap<String, Field> fields;

    //User Records
    int i = 0;

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("u_id" ,Field.create(++i));
    fields.put("name", Field.create("Alice"));
    fields.put("address", Field.create("100 First Street, Sunnyvale, CA."));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("USER").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("u_id" ,Field.create(++i));
    fields.put("name", Field.create("Zach"));
    fields.put("address", Field.create("200 Second Street, Sunnyvale, CA."));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("USER").getRight().add(record);

    record = RecordCreator.create();
    fields.put("u_id" ,Field.create(++i));
    fields.put("name", Field.create("Jack"));
    fields.put("address", Field.create("300 Third Street, Sunnyvale, CA."));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("USER").getRight().add(record);


    //Product Records
    i = 0;

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("p_id" ,Field.create(++i));
    fields.put("name", Field.create("Coconut Chips"));
    fields.put("manufacturer", Field.create("Dang"));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("PRODUCT").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("p_id" ,Field.create(++i));
    fields.put("name", Field.create("Bluberry Bar"));
    fields.put("manufacturer", Field.create("Luna"));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("PRODUCT").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("p_id" ,Field.create(++i));
    fields.put("name", Field.create("Dark Chocolate Peanut Butter Bar"));
    fields.put("manufacturer", Field.create("Kind"));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("PRODUCT").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("p_id" ,Field.create(++i));
    fields.put("name", Field.create("Oats and Honey Bar"));
    fields.put("manufacturer", Field.create("Nature Valley"));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("PRODUCT").getRight().add(record);


    //ORDER_TBL Records
    i = 0;

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("o_id" ,Field.create(++i));
    fields.put("u_id", Field.create(1));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ORDER_TBL").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("o_id" ,Field.create(++i));
    fields.put("u_id", Field.create(2));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ORDER_TBL").getRight().add(record);


    //Items Records
    long currentTime = System.currentTimeMillis();

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("time_id" ,Field.create(currentTime));
    fields.put("o_id", Field.create(1));
    fields.put("p_id", Field.create(1));
    fields.put("quantity", Field.create(2));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ITEMS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("time_id" ,Field.create(currentTime + 1 ));
    fields.put("o_id", Field.create(1));
    fields.put("p_id", Field.create(2));
    fields.put("quantity", Field.create(3));

    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ITEMS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("time_id" ,Field.create(currentTime + 2));
    fields.put("o_id" ,Field.create(2));
    fields.put("p_id", Field.create(1));
    fields.put("quantity", Field.create(4));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ITEMS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("time_id" ,Field.create(currentTime + 3));
    fields.put("o_id" ,Field.create(2));
    fields.put("p_id", Field.create(3));
    fields.put("quantity", Field.create(2));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ITEMS").getRight().add(record);

    record = RecordCreator.create();
    fields = new LinkedHashMap<>();
    fields.put("time_id" ,Field.create(currentTime + 4));
    fields.put("o_id" ,Field.create(2));
    fields.put("p_id", Field.create(4));
    fields.put("quantity", Field.create(1));
    record.set(Field.createListMap(fields));
    TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get("ITEMS").getRight().add(record);
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    populateRecords();

    try (Statement statement = connection.createStatement()) {
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");

      //USER TABLE
      statement.addBatch("CREATE TABLE TEST.USER (u_id INT PRIMARY KEY, name varchar(100), address varchar(1000));");

      //PRODUCT TABLE
      statement.addBatch("CREATE TABLE TEST.PRODUCT (p_id INT PRIMARY KEY, name varchar(100), manufacturer varchar(1000));");

      //ORDER_TBL TABLE
      statement.addBatch("CREATE TABLE TEST.ORDER_TBL (o_id INT PRIMARY KEY, u_id INT, FOREIGN KEY (u_id) REFERENCES USER(u_id))");

      //ITEMS TABLE
      //We do not support composite keys so for now the primary key here is a timestamp.
      statement.addBatch("CREATE TABLE TEST.ITEMS (" +
          "time_id long PRIMARY KEY, o_id INT," +
          " p_id INT, quantity int," +
          " FOREIGN KEY (o_id) REFERENCES ORDER_TBL(o_id), FOREIGN KEY (p_id) REFERENCES PRODUCT(p_id))");

      statement.executeBatch();
    }
    for (Pair<String, ArrayList<Record>> value : TABLE_TO_TEMPLATE_AND_RECORDS_MAP.values()) {
      insertRows(value.getLeft(), value.getRight());
    }
  }

  @AfterClass
  public static void deleteTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      List<String> tablesToDelete = new ArrayList<>(TABLE_TO_TEMPLATE_AND_RECORDS_MAP.keySet());
      //Reverse order of tables for deletion.
      Collections.reverse(tablesToDelete);
      for (String tableToDelete  : tablesToDelete) {
        statement.addBatch("DROP TABLE TEST." + tableToDelete);
      }
      statement.executeBatch();
    }
  }

  private String runCheckAndReturnOffset(SourceRunner runner, String offset, String table) throws Exception {
    StageRunner.Output output = runner.runProduce(offset, 1000);
    List<Record> expectedRecords = TABLE_TO_TEMPLATE_AND_RECORDS_MAP.get(table).getRight();
    List<Record> actualRecords =  output.getRecords().get("a");
    checkRecords(expectedRecords, actualRecords);
    return output.getNewOffset();
  }

  @Test
  public void testReferentialOrdering() throws Exception {
    TableConfigBean tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .tableOrderStrategy(TableOrderStrategy.REFERENTIAL_CONSTRAINTS)
        .build();

    SourceRunner runner = new SourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    runner.runInit();

    //USER and PRODUCT are the only tables which do not depend on Anything, but PRODUCT gets scheduled first
    //because of the alphabetical ORDER_TBL, then USER, then ORDER_TBL, then ITEM.
    String offset = "";
    try {
      offset = runCheckAndReturnOffset(runner, offset, "PRODUCT");
      offset = runCheckAndReturnOffset(runner, offset, "USER");
      offset = runCheckAndReturnOffset(runner, offset, "ORDER_TBL");
      runCheckAndReturnOffset(runner, offset, "ITEMS");
    } finally {
      runner.runDestroy();
    }
  }


}
