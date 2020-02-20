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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.testing.RandomTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class CompositeKeysIT extends BaseTableJdbcSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeKeysIT.class);
  private static final List<Record> MULTIPLE_INT_COMPOSITE_RECORDS = new ArrayList<>();
  private static final Random RANDOM = RandomTestUtils.getRandom();
  private static final String LOG_TEMPLATE =
      "Batches Read Till Now : {}," +
          " Record Read Till Now: {}," +
          " Remaining Records : {}, Current Batch Size: {}, Output Record Size : {}";
  private static final String MULTIPLE_INT_COMPOSITE_INSERT_TEMPLATE = "INSERT into TEST.%s values (%s, %s, %s, '%s')";
  private static final String TABLE_NAME = "MULTIPLE_INT_PRIMARY";

  private static Record createMultipleIntCompositePrimaryKeyRecord(int id_1, int id_2, int id_3, String stringCol) {
    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fieldMap =
        new LinkedHashMap<>(ImmutableMap.of(
            "id_1", Field.create(id_1),
            "id_2", Field.create(id_2),
            "id_3", Field.create(id_3),
            "stringcol", Field.create(stringCol)
        ));
    record.set(Field.createListMap(fieldMap));
    return record;
  }

  @BeforeClass
  public static void setupTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.addBatch(
          "CREATE TABLE TEST." +
              TABLE_NAME +
              " (" +
              " id_1 INT NOT NULL," +
              " id_2 INT NOT NULL," +
              " id_3 INT NOT NULL," +
              " stringcol varchar(500)," +
              " PRIMARY KEY (id_1, id_2, id_3)" +
              ")"
      );
      //Totally create 5 * 5 * 5 = 125 records
      for (int i = 1; i <= 5; i++) {
        for (int j = 1; j <= 5; j++) {
          for (int k = 1; k <= 5; k++) {
            MULTIPLE_INT_COMPOSITE_RECORDS.add(createMultipleIntCompositePrimaryKeyRecord(i, j, k, UUID.randomUUID().toString()));
          }
        }
      }

      List<Record> recordsToInsert = new ArrayList<>(MULTIPLE_INT_COMPOSITE_RECORDS);
      //Shuffled to make sure we order and get this properly
      Collections.shuffle(recordsToInsert);
      for (Record recordToInsert : recordsToInsert) {
        statement.addBatch(
            String.format(
                MULTIPLE_INT_COMPOSITE_INSERT_TEMPLATE,
                TABLE_NAME,
                recordToInsert.get("/id_1").getValue(),
                recordToInsert.get("/id_2").getValue(),
                recordToInsert.get("/id_3").getValue(),
                recordToInsert.get("/stringcol").getValue()
            )
        );
      }
      statement.executeBatch();
    }
  }

  @AfterClass
  public static void dropTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format(DROP_STATEMENT_TEMPLATE, database, TABLE_NAME));
    }
  }

  @Test
  public void testCompositePrimaryKeys() throws Exception {
    int recordsRead = 0, noOfBatches = 0, totalNoOfRecords = MULTIPLE_INT_COMPOSITE_RECORDS.size();
    Map<String, String> offsets = Collections.emptyMap();
    while (recordsRead < totalNoOfRecords) {
      TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
          .tablePattern("%")
          .schema(database)
          .build();

      TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
          .tableConfigBeans(ImmutableList.of(tableConfigBean))
          .quoteChar(QuoteChar.BACKTICK)
          .build();

      PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
          .addOutputLane("a").build();
      runner.runInit();

      try {
        //Random batch size (Making sure at least batch size is 1)
        int bound = totalNoOfRecords - recordsRead - 1;
        int batchSize = (bound == 0)? 1: RANDOM.nextInt(bound) + 1;

        JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);

        runner.runProduce(offsets, batchSize, callback);

        List<List<Record>> batchRecords = callback.waitForAllBatchesAndReset();

        List<Record> actualRecords = batchRecords.get(0);

        List<Record> expectedRecords = MULTIPLE_INT_COMPOSITE_RECORDS.subList(recordsRead, recordsRead + batchSize);

        Comparator<Record> recordComp = Comparator.comparingInt(
            (Record r)->r.get("/ID_1").getValueAsInteger())
            .thenComparingInt(r->r.get("/ID_2").getValueAsInteger())
            .thenComparingInt(r->r.get("/ID_3").getValueAsInteger());

        checkRecords(TABLE_NAME, expectedRecords, actualRecords, null, null);

        recordsRead = recordsRead + batchSize;
        noOfBatches++;

        offsets = new HashMap<>(runner.getOffsets());

        LOG.info(LOG_TEMPLATE, noOfBatches, recordsRead, (totalNoOfRecords - recordsRead), batchSize, actualRecords.size());
      } finally {
        runner.runDestroy();
      }
    }
  }

  @Test
  public void testPartitioningMode() throws Exception {
    TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern("%")
        .schema(database)
        .partitioningMode(PartitioningMode.REQUIRED)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .quoteChar(QuoteChar.BACKTICK)
        .build();

    validateAndAssertConfigIssue(tableJdbcSource, JdbcErrors.JDBC_100, TABLE_NAME);

    tableConfigBean.partitioningMode = PartitioningMode.BEST_EFFORT;

    validateAndAssertNoConfigIssues(tableJdbcSource);
  }
}
