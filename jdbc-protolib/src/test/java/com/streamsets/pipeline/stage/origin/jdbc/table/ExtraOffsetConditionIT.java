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
import com.streamsets.pipeline.lib.jdbc.multithread.TableJdbcRunnable;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(value = Parameterized.class)
@PowerMockIgnore({
    "javax.security.*",
    "javax.management.*",
    "jdk.internal.reflect.*"
})
@PrepareForTest(value = {
    TableJdbcSource.class,
    TableJdbcRunnable.class
})
public class ExtraOffsetConditionIT extends BaseTableJdbcSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(ExtraOffsetConditionIT.class);
  private static final int BATCHES = 5;
  private static final String RANDOM_STRING_COLUMN = "random_string";

  //This will basically hold different date times that can be used for
  //creating offset fields
  private static final Map<Field.Type, List<Object>> DATE_TIMES =
      ImmutableMap.of(
          Field.Type.DATE, getDifferentDateTimes(Field.Type.DATE),
          Field.Type.TIME, getDifferentDateTimes(Field.Type.TIME),
          Field.Type.DATETIME, getDifferentDateTimes(Field.Type.DATETIME),
          Field.Type.LONG, getDifferentDateTimes(Field.Type.LONG)
      );

  private final String tableName;
  private final Map<String, Field.Type> transactionOffsetFields;
  private final String extraOffsetConditions;

  //Each element in the index is a list of records for each batch.
  private List<List<Record>> batchRecords = new ArrayList<>();

  public ExtraOffsetConditionIT(
      String tableName,
      Map<String, Field.Type> transactionOffsetFields,
      String extraOffsetConditions
  ) {
    this.tableName = tableName;
    this.transactionOffsetFields = transactionOffsetFields;
    this.extraOffsetConditions = extraOffsetConditions;
  }

  //Generate different date, time, date time and long for usage in offset column
  //Totally generate 5 different values for each type (ending < currentDateTime)
  private static List<Object> getDifferentDateTimes(Field.Type type) {
    List<Object> dateTimes = new ArrayList<>();
    Calendar calendar = Calendar.getInstance();
    switch (type) {
      case DATE:
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DATE,  -(BATCHES + 1));
        for (int i = 0 ; i < 5; i++) {
          //Increment 1 DAY for each value
          calendar.add(Calendar.DATE, 1);
          dateTimes.add(calendar.getTime());
        }
        break;
      case TIME:
        calendar.set(Calendar.YEAR, 1970);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MILLISECOND, -(BATCHES + 1));
        for (int i = 0 ; i < 5; i++) {
          //Increment 1 millisecond for each value
          calendar.add(Calendar.MILLISECOND, 1);
          dateTimes.add(calendar.getTime());
        }
        break;
      case DATETIME:
      case LONG:
        calendar.add(Calendar.MILLISECOND, -(BATCHES + 1));
        for (int i = 0 ; i < 5; i++) {
          //Increment 1 millisecond for each value
          calendar.add(Calendar.MILLISECOND, 1);
          Date currentDateTime = calendar.getTime();
          dateTimes.add(type == Field.Type.LONG ? currentDateTime.getTime() : currentDateTime);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported Field Type {}" + type);
    }
    return dateTimes;
  }

  private List<List<Record>> createRecords(Map<String, Field.Type> offsetFieldToTypes) {
    for (int i = 0 ; i < BATCHES; i++) {
      List<Record> records = new ArrayList<>();
      Map<String, Field> fieldMap = new LinkedHashMap<>();
      for (Map.Entry<String, Field.Type> offsetFieldToTypeEntry : offsetFieldToTypes.entrySet()) {
        Object currentValue = DATE_TIMES.get(offsetFieldToTypeEntry.getValue()).get(i);
        fieldMap.put(offsetFieldToTypeEntry.getKey(), Field.create(offsetFieldToTypeEntry.getValue(), currentValue));
      }
      fieldMap.put(RANDOM_STRING_COLUMN, Field.create(UUID.randomUUID().toString()));
      //Every value gets included twice
      //So we actually have two records with same offset field values.
      //And when selecting we should see both records.
      for (int j = 0 ; j < 2; j++) {
        Record record = RecordCreator.create();
        record.set(Field.create(Field.Type.LIST_MAP, fieldMap));
        records.add(record);
      }
      batchRecords.add(records);
    }
    return batchRecords;
  }

  @Parameterized.Parameters(name = "Table Name : ({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(
        new Object[][]{
            {
                "TRANSACTION_DATE",
                ImmutableMap.of("transaction_date", Field.Type.DATE),
                //basically says less than current date
                "${offset:column(0)} < '${YYYY()}-${MM()}-${DD()}'"
            },
            {
                "TRANSACTION_TIME",
                ImmutableMap.of("transaction_time", Field.Type.TIME),
                //less than current time
                "${offset:column(0)} < '${hh()}:${mm()}:${ss()}.${SSS()}'"
            },
            {
                "TRANSACTION_DATETIME",
                ImmutableMap.of("transaction_datetime", Field.Type.DATETIME),
                //less than current date time
                "${offset:column(0)} < '${YYYY()}-${MM()}-${DD()} ${hh()}:${mm()}:${ss()}.${SSS()}'"
            },
            {
                "TRANSACTION_LONG",
                ImmutableMap.of("transaction_long", Field.Type.LONG),
                //less than current date time in milliseconds
                "${offset:column(0)} < ${time:dateTimeToMilliseconds(time:now())}"
            },
            {
                "TRANSACTION_DATE_AND_TIME",
                ImmutableMap.of("transaction_date", Field.Type.DATE, "transaction_time", Field.Type.TIME),
                //less than current Date or equal to current date but less than current time.
                "${offset:column(0)} < '${YYYY()}-${MM()}-${DD()}'" +
                    " OR (${offset:column(0)} = '${YYYY()}-${MM()}-${DD()}'" +
                    " AND ${offset:column(1)} < '${hh()}:${mm()}:${ss()}.${SSS()}')"
            }
        }
    );
  }

  @Before
  public void setupTables() throws Exception {
    try (Statement statement = connection.createStatement()) {

      Map<String, String> fieldsForCreate = new LinkedHashMap<>();

      for (Map.Entry<String, Field.Type> offsetFieldEntry : transactionOffsetFields.entrySet()) {
        fieldsForCreate.put(offsetFieldEntry.getKey(), FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(offsetFieldEntry.getValue()));
      }
      //extra field (other than offset column.)
      fieldsForCreate.put(RANDOM_STRING_COLUMN, FIELD_TYPE_TO_SQL_TYPE_AND_STRING.get(Field.Type.STRING));

      statement.addBatch(
          getCreateStatement(
              "TEST",
              tableName,
              Collections.<String, String>emptyMap(),
              fieldsForCreate
          )
      );

      createRecords(transactionOffsetFields);

      List<Record> recordsToInsert = new ArrayList<>();
      for (List<Record> records : batchRecords) {
        recordsToInsert.addAll(records);
      }

      for (Record recordToInsert : recordsToInsert) {
        Map<String, Field> fields = recordToInsert.get().getValueAsListMap();
        statement.addBatch(getInsertStatement(database, tableName, fields.values()));
      }
      statement.executeBatch();
    }
  }

  @After
  public void deleteTables() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format(DROP_STATEMENT_TEMPLATE, database, tableName));
    }
  }

  /**
   * This method helps manipulate the current time to be less than what we have
   * stored in {@link #batchRecords} for the next batch
   * So every batch will get 2 records.
   */
  private void setTimeContextForProduce(TableJdbcSource tableJdbcSource, int batchNumber) {
    Calendar finalCalendar = Calendar.getInstance();
    Date currentDateTime = finalCalendar.getTime();

    finalCalendar.set(Calendar.YEAR, 1970);
    finalCalendar.set(Calendar.MONTH, 0);
    finalCalendar.set(Calendar.DAY_OF_MONTH, 1);
    finalCalendar.set(Calendar.HOUR_OF_DAY, 0);
    finalCalendar.set(Calendar.MINUTE, 0);
    finalCalendar.set(Calendar.SECOND, 0);
    finalCalendar.set(Calendar.MILLISECOND, 0);

    for (Map.Entry<String, Field.Type> offsetFieldToTypeEntry : transactionOffsetFields.entrySet()) {
      String fieldPath = offsetFieldToTypeEntry.getKey();

      int year = finalCalendar.get(Calendar.YEAR);
      int month = finalCalendar.get(Calendar.MONTH);
      int day = finalCalendar.get(Calendar.DAY_OF_MONTH);
      int hour = finalCalendar.get(Calendar.HOUR_OF_DAY);
      int minutes = finalCalendar.get(Calendar.MINUTE);
      int seconds = finalCalendar.get(Calendar.SECOND);
      int milliSeconds = finalCalendar.get(Calendar.MILLISECOND);

      //Basically use the next timestamp which should be picked for the next batch
      //and pretty much use it as current time (which the extra condition will evaluate to)
      //For the last batch use the current dateTime.
      Date date = (batchNumber < BATCHES - 1) ?
          batchRecords.get(batchNumber + 1).get(0).get().getValueAsListMap().get(fieldPath).getValueAsDatetime()
          : currentDateTime;

      Calendar temp = Calendar.getInstance();
      temp.setTime(date);

      switch (offsetFieldToTypeEntry.getValue()) {
        case DATE:
          year = temp.get(Calendar.YEAR);
          month = temp.get(Calendar.MONTH);
          day = temp.get(Calendar.DAY_OF_MONTH);
          break;
        case TIME:
          hour = temp.get(Calendar.HOUR_OF_DAY);
          minutes = temp.get(Calendar.MINUTE);
          seconds = temp.get(Calendar.SECOND);
          milliSeconds = temp.get(Calendar.MILLISECOND);
          break;
        case DATETIME:
        case LONG:
          year = temp.get(Calendar.YEAR);
          month = temp.get(Calendar.MONTH);
          day = temp.get(Calendar.DAY_OF_MONTH);
          hour = temp.get(Calendar.HOUR_OF_DAY);
          minutes = temp.get(Calendar.MINUTE);
          seconds = temp.get(Calendar.SECOND);
          milliSeconds = temp.get(Calendar.MILLISECOND);
          break;
        default:
          throw new IllegalArgumentException("Unsupported Field Type {}" + offsetFieldToTypeEntry.getValue());
      }
      finalCalendar.set(Calendar.YEAR, year);
      finalCalendar.set(Calendar.MONTH, month);
      finalCalendar.set(Calendar.DAY_OF_MONTH, day);
      finalCalendar.set(Calendar.HOUR_OF_DAY, hour);
      finalCalendar.set(Calendar.MINUTE, minutes);
      finalCalendar.set(Calendar.SECOND, seconds);
      finalCalendar.set(Calendar.MILLISECOND, milliSeconds);
    }
    PowerMockito.replace(
        MemberMatcher.method(
            TableJdbcRunnable.class,
            "initTableEvalContextForProduce",
            TableJdbcELEvalContext.class,
            TableRuntimeContext.class,
            Calendar.class
        )
    ).with((proxy, method, args) -> {
      args[2] = finalCalendar;
      return method.invoke(proxy, args);
    });
  }

  @Test
  public void testExtraOffsetConditions() throws Exception {
    List<String> offsetColumns = new ArrayList<>();
    for (String partitionColumn : transactionOffsetFields.keySet()) {
      offsetColumns.add(partitionColumn.toUpperCase());
    }
    TableConfigBeanImpl tableConfigBean =  new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
        .tablePattern(tableName)
        .schema(database)
        .overrideDefaultOffsetColumns(true)
        .offsetColumns(offsetColumns)
        .extraOffsetColumnConditions(extraOffsetConditions)
        .partitioningMode(PartitioningMode.DISABLED)
        .build();

    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(ImmutableList.of(tableConfigBean))
        .build();
    Map<String, String> offsets = Collections.emptyMap();
    for (int i = 0; i < BATCHES; i++) {
      LOG.info("Starting iteration {} out of {}", i, BATCHES);
      PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
          .addOutputLane("a").build();
      runner.runInit();
      JdbcPushSourceTestCallback callback = new JdbcPushSourceTestCallback(runner, 1);
      try {
        final List<Record> expectedRecords = batchRecords.get(i);
        setTimeContextForProduce(tableJdbcSource, i);
        runner.runProduce(offsets, 1000, callback);
        List<Record> actualRecords = callback.waitForAllBatchesAndReset().get(0);
        checkRecords(expectedRecords, actualRecords);
        offsets = runner.getOffsets();
      } finally {
        runner.runDestroy();
      }
    }
  }
}
