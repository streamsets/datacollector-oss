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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.RandomTestUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public abstract class BaseTableJdbcSourceIT {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTableJdbcSourceIT.class);

  protected static final String USER_NAME = "sa";
  protected static final String PASSWORD = "sa";
  protected static final String database = "TEST";
  protected static final String JDBC_URL = "jdbc:h2:mem:" + database;// + ";MVCC=TRUE";
  protected static final String CREATE_STATEMENT_TEMPLATE = "CREATE TABLE %s.%s ( %s )";
  protected static final String INSERT_STATEMENT_TEMPLATE = "INSERT INTO %s.%s values ( %s )";
  protected static final String DROP_STATEMENT_TEMPLATE = "DROP TABLE %s.%s CASCADE";
  protected static final Joiner COMMA_SPACE_JOINER = Joiner.on(", ");
  protected static final Random RANDOM = RandomTestUtils.getRandom();

  protected static final Map<Field.Type, String> FIELD_TYPE_TO_SQL_TYPE_AND_STRING =
      ImmutableMap.<Field.Type, String>builder()
          .put(Field.Type.BOOLEAN, "BIT")
          .put(Field.Type.CHAR, "CHAR")
          .put(Field.Type.BYTE, "TINYINT")
          .put(Field.Type.SHORT, "SMALLINT")
          .put(Field.Type.INTEGER, "INTEGER")
          .put(Field.Type.LONG, "BIGINT")
          //TO please h2 to return a Types.REAL which can be converted to FLOAT
          //Types.FLOAT gets returned as double.
          .put(Field.Type.FLOAT, "REAL")
          .put(Field.Type.DOUBLE, "DOUBLE")
          .put(Field.Type.DECIMAL, "DECIMAL(20, 10)")
          .put(Field.Type.STRING, "varchar(100)")
          .put(Field.Type.BYTE_ARRAY, "BINARY")
          .put(Field.Type.DATE, "DATE")
          .put(Field.Type.TIME, "TIME")
          .put(Field.Type.DATETIME, "TIMESTAMP")
          .put(Field.Type.ZONED_DATETIME, "TIMESTAMP WITH TIME ZONE")
          .build();

  protected static Connection connection;

  protected static void validateAndAssertNoConfigIssues(TableJdbcSource tableJdbcSource) throws StageException {
    validateAndAssertConfigIssue(tableJdbcSource, null, null);
  }

  protected static void validateAndAssertConfigIssue(
      TableJdbcSource tableJdbcSource,
      ErrorCode expectedErrorCode,
      String expectedInErrorMessage
  ) throws StageException {

    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    List<Stage.ConfigIssue> configIssues = runner.runValidateConfigs();
    if (expectedErrorCode == null) {
      assertThat(configIssues, hasSize(0));
    } else {
      assertThat(configIssues, hasSize(1));
      Stage.ConfigIssue issue = configIssues.get(0);

      final ErrorMessage errorMsg = (ErrorMessage) Whitebox.getInternalState(issue, "message");
      assertThat(errorMsg, notNullValue());
      Assert.assertEquals(
          expectedErrorCode.getCode(),
          errorMsg.getErrorCode()
      );

      if (expectedInErrorMessage != null) {
        assertThat(errorMsg.getLocalized(), containsString(expectedInErrorMessage));
      }
    }
  }


  static class JdbcPushSourceTestCallback implements PushSourceRunner.Callback {
    private final PushSourceRunner pushSourceRunner;
    private final List<List<Record>> batchRecords;
    private final AtomicInteger batchesProduced;
    private final int numberOfBatches;

    JdbcPushSourceTestCallback(PushSourceRunner pushSourceRunner, int numberOfBatches) {
      this.pushSourceRunner = pushSourceRunner;
      this.batchRecords = new ArrayList<>(numberOfBatches);
      this.numberOfBatches = numberOfBatches;
      this.batchesProduced = new AtomicInteger(0);
    }

    synchronized List<List<Record>> waitForAllBatchesAndReset() {
      try {
        pushSourceRunner.waitOnProduce();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      List<List<Record>> records = ImmutableList.copyOf(batchRecords);
      Assert.assertEquals(numberOfBatches, records.size());
      batchRecords.clear();
      batchesProduced.set(0);
      return records;
    }

    @Override
    public void processBatch(StageRunner.Output output) {
      List<Record> records = output.getRecords().get("a");
      if (!records.isEmpty()) {
        batchRecords.add(batchesProduced.get(), records);
        if (batchesProduced.incrementAndGet() == numberOfBatches) {
          pushSourceRunner.setStop();
        }
      }
    }
  }

  @BeforeClass
  public static void setup() throws SQLException {
    DataCollectorServicesUtils.loadDefaultServices();

    connection = DriverManager.getConnection(JDBC_URL, USER_NAME, PASSWORD);
    try (Statement statement = connection.createStatement()) {
      //If not exists is skipped because some of the databases do not support that.
      statement.execute("CREATE SCHEMA TEST");
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      //If not exists is skipped because some of the databases do not support that.
      statement.execute("DROP SCHEMA TEST");
    }
    connection.close();
  }


  protected static Date getRandomDateTime(Field.Type type) {
    Calendar calendar = Calendar.getInstance();
    //1990-2020
    calendar.set(Calendar.YEAR, RANDOM.nextInt(30) + 1990);
    calendar.set(Calendar.MONTH, RANDOM.nextInt(11) + 1);
    calendar.set(Calendar.DAY_OF_MONTH, RANDOM.nextInt(25));
    calendar.set(Calendar.HOUR_OF_DAY, RANDOM.nextInt(24));
    calendar.set(Calendar.MINUTE, RANDOM.nextInt(60));
    calendar.set(Calendar.SECOND, RANDOM.nextInt(60));
    calendar.set(Calendar.MILLISECOND, RANDOM.nextInt(1000));
    if (type == Field.Type.DATE) {
      //zero out time part
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
    } else if (type == Field.Type.TIME) {
      //unset
      calendar.set(Calendar.YEAR, 1970);
      calendar.set(Calendar.MONTH, 0);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
    }
    return calendar.getTime();
  }

  protected static Object generateRandomData(Field.Type fieldType) {
    switch(fieldType) {
      case DATE:
      case DATETIME:
      case TIME:
        return getRandomDateTime(fieldType);
      case ZONED_DATETIME:
        return ZonedDateTime.now();
      case DOUBLE:
        return RANDOM.nextDouble();
      case FLOAT:
        return RANDOM.nextFloat();
      case SHORT:
        return (short) RANDOM.nextInt(Short.MAX_VALUE + 1);
      case INTEGER:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case CHAR:
        return UUID.randomUUID().toString().charAt(0);
      case STRING:
        return UUID.randomUUID().toString();
      case DECIMAL:
        return new BigDecimal(BigInteger.valueOf(RANDOM.nextLong() % (long)Math.pow(10, 20)), 10);
      default:
        return null;
    }
  }


  protected static void setParamsToPreparedStatement(
      PreparedStatement ps,
      int paramIdx,
      int sqlType,
      Object value
  ) throws SQLException {
    switch (sqlType) {
      case Types.DATE:
        ps.setDate(paramIdx, new java.sql.Date(((Date)value).getTime()));
        break;
      case Types.TIME:
        ps.setTime(paramIdx, new java.sql.Time(((Date)value).getTime()));
        break;
      case Types.TIMESTAMP:
        ps.setTimestamp(paramIdx, new java.sql.Timestamp(((Date)value).getTime()));
        break;
      default:
        ps.setObject(paramIdx, value);
    }
  }

  protected static String getStringRepOfFieldValueForInsert(Field field) {
    switch (field.getType()) {
      case BYTE_ARRAY:
        //Do a hex encode.
        return Hex.encodeHexString(field.getValueAsByteArray());
      case BYTE:
        return String.valueOf(field.getValueAsInteger());
      case TIME:
        return DateFormatUtils.format(field.getValueAsDate(), "HH:mm:ss.SSS");
      case DATE:
        return DateFormatUtils.format(field.getValueAsDate(), "yyyy-MM-dd");
      case DATETIME:
        return DateFormatUtils.format(field.getValueAsDate(), "yyyy-MM-dd HH:mm:ss.SSS");
      case ZONED_DATETIME:
        return DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(
            field.getValueAsZonedDateTime().toInstant().toEpochMilli()
        );
      default:
        return String.valueOf(field.getValue());
    }
  }


  protected static String getCreateStatement(
      String schemaName,
      String tableName,
      Map<String, String> offsetFields,
      Map<String, String> otherFields
  ) {
    return getCreateStatement(schemaName, tableName, offsetFields, otherFields, true);
  }

  protected static String getCreateStatement(
      String schemaName,
      String tableName,
      Map<String, String> offsetFields,
      Map<String, String> otherFields,
      boolean usePrimaryKey
  ) {
    List<String> fieldFormats = new ArrayList<>();
    for (Map.Entry<String, String> offsetFieldEntry : offsetFields.entrySet()) {
      Assert.assertNotNull("Null Value for - " + offsetFieldEntry, offsetFieldEntry.getValue());
      fieldFormats.add(offsetFieldEntry.getKey() + " " + offsetFieldEntry.getValue() + " NOT NULL");
    }

    for (Map.Entry<String, String> otherFieldEntry : otherFields.entrySet()) {
      Assert.assertNotNull("Null Value for - " + otherFieldEntry, otherFieldEntry.getValue());
      fieldFormats.add(otherFieldEntry.getKey() + " " + otherFieldEntry.getValue());
    }

    if (!offsetFields.isEmpty()) {
      if (usePrimaryKey) {
        fieldFormats.add("PRIMARY KEY(" + COMMA_SPACE_JOINER.join(offsetFields.keySet()) + ")");
      }
    }

    String createQuery = String.format(CREATE_STATEMENT_TEMPLATE, schemaName, tableName, COMMA_SPACE_JOINER.join(fieldFormats));
    LOG.info("Created Query : " + createQuery);

    return createQuery;
  }

  protected static String getInsertStatement(String schemaName, String tableName, Collection<Field> fields) {
    List<String> fieldFormats = new ArrayList<>();
    for (Field field : fields) {
      String fieldFormat =
          (
              field.getType().isOneOf(
                  Field.Type.DATE,
                  Field.Type.TIME,
                  Field.Type.DATETIME,
                  Field.Type.ZONED_DATETIME,
                  Field.Type.CHAR,
                  Field.Type.STRING
              )
          )? "'"+ getStringRepOfFieldValueForInsert(field) +"'" : getStringRepOfFieldValueForInsert(field);

      Assert.assertNotNull(fieldFormat);
      fieldFormats.add(fieldFormat);
    }
    String insertQuery = String.format(INSERT_STATEMENT_TEMPLATE, schemaName, tableName, COMMA_SPACE_JOINER.join(fieldFormats));
    LOG.info("Created Query : " + insertQuery);
    return insertQuery;
  }


  protected static void insertRows(String insertTemplate, List<Record> records) throws SQLException {
    try (Statement st = connection.createStatement()) {
      for (Record record : records) {
        List<String> values = new ArrayList<>();
        for (String fieldPath : record.getEscapedFieldPaths()) {
          //Skip root field
          if (!fieldPath.equals("")) {
            values.add(getStringRepOfFieldValueForInsert(record.get(fieldPath)));
          }
        }
        st.addBatch(String.format(insertTemplate, values.toArray()));
      }
      st.executeBatch();
    }
  }


  private static void checkField(String fieldPath, Field expectedField, Field actualField) {
    String errorString = String.format("Error in Field Path: %s", fieldPath);
    Assert.assertEquals(errorString, expectedField.getType(), actualField.getType());
    errorString = errorString + " of type: " + expectedField.getType().name();
    switch (expectedField.getType()) {
      case MAP:
      case LIST_MAP:
        Map<String, Field> expectedFieldMap = expectedField.getValueAsMap();
        Map<String, Field> actualFieldMap = actualField.getValueAsMap();
        Assert.assertEquals(errorString, expectedFieldMap.keySet().size(), actualFieldMap.keySet().size());
        for (Map.Entry<String, Field> entry : actualFieldMap.entrySet()) {
          String actualFieldName = entry.getKey().toLowerCase();
          Assert.assertNotNull(errorString, expectedFieldMap.get(actualFieldName));
          checkField(fieldPath + "/" + actualFieldName, expectedFieldMap.get(actualFieldName), entry.getValue());
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
      case ZONED_DATETIME:
        // H2 seems to throw away millis by default, so just make sure they are within 1 second
        final long millisDifference = expectedField.getValueAsZonedDateTime().toInstant().toEpochMilli()
            - actualField.getValueAsZonedDateTime().toInstant().toEpochMilli();
        assertThat(errorString, Math.abs(millisDifference), lessThanOrEqualTo(1000l));
        break;
      default:
        Assert.assertEquals(errorString, expectedField.getValue(), actualField.getValue());
    }
  }

  static void checkRecords(List<Record> expectedRecords, List<Record> actualRecords) {
    checkRecords("(null)", expectedRecords, actualRecords, null, null);
  }

  static void checkRecords(
      String tableName,
      List<Record> expectedRecords,
      List<Record> actualRecords,
      Comparator<Record> actualRecordComparator
      ) {
    checkRecords("(null)", expectedRecords, actualRecords, actualRecordComparator, null);
  }

  static void checkRecords(
      String tableName,
      List<Record> expectedRecords,
      List<Record> actualRecords,
      Comparator<Record> actualRecordComparator,
      Comparator<Record> expectedRecordComparator
  ) {
    Assert.assertEquals(
        String.format("Record Size for table %s does not match.", tableName),
        expectedRecords.size(),
        actualRecords.size()
    );

    List<Record> actualRecordsSorted = new ArrayList<>(actualRecords);

    if (actualRecordComparator != null) {
      Collections.sort(actualRecordsSorted, actualRecordComparator);
    }

    for (int i = 0; i < actualRecordsSorted.size(); i++) {
      Record actualRecord = actualRecordsSorted.get(i);
      Record expectedRecord = expectedRecords.get(i);
      checkField("", expectedRecord.get(), actualRecord.get());
    }
  }
}
