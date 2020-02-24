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
package com.streamsets.pipeline.stage.processor.jdbcmetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcSchemaReader;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class TestJdbcMetadata {
  private static final String CANNOT_CONNECT_TO_SPECIFIED_DATABASE = "Cannot connect to specified database";
  private static final String BAD_CONNECTION_STRING = "bad connection string";
  private static final String SCALE = "2";
  private static final String PRECISION = "10";
  private final String username = "sa";
  private final String password = "sa";
  private final String database = "TEST";
  private final String tableName = "TEST_TABLE";

  private final String h2ConnectionString = "jdbc:h2:mem:" + database;

  private Connection connection = null;
  private static JdbcUtil jdbcUtil;

  // Records to create
  private Map<String, Pair<Field.Type, Object>> fieldMap1 = ImmutableMap.<String, Pair<Field.Type, Object>>builder()
      .put("column1", Pair.of(Field.Type.STRING, "StringData"))
      .put("column2", Pair.of(Field.Type.INTEGER, 1))
      .build();

  private Map<String, Pair<Field.Type, Object>> fieldMap2 = ImmutableMap.<String, Pair<Field.Type, Object>>builder()
      .put("column1", Pair.of(Field.Type.STRING, "More String Data"))
      .put("column2", Pair.of(Field.Type.INTEGER, 1))
      .put("column3", Pair.of(Field.Type.DOUBLE, 2.3))
      .put("column4", Pair.of(Field.Type.BOOLEAN, true))
      .put("column5", Pair.of(Field.Type.DECIMAL, BigDecimal.valueOf(345, 2)))
      .build();

  private Map<String, Pair<Field.Type, Object>> fieldMap3 = ImmutableMap.<String, Pair<Field.Type, Object>>builder()
      .put("column1", Pair.of(Field.Type.STRING, "More String Data"))
      .put("column2", Pair.of(Field.Type.INTEGER, 1))
      .put("column3", Pair.of(Field.Type.BOOLEAN, true))
      .put("column4", Pair.of(Field.Type.DOUBLE, 2.3))
      .build();

  // Expected type mappings
  private Map<Integer, Field.Type> typeMap = ImmutableMap.<Integer, Field.Type>builder()
      .put(Types.BOOLEAN, Field.Type.BOOLEAN)
      .put(Types.BIT, Field.Type.BOOLEAN)
      .put(Types.CHAR, Field.Type.CHAR)
      .put(Types.DATE, Field.Type.DATE)
      .put(Types.TIMESTAMP, Field.Type.DATETIME)
      .put(Types.TIME, Field.Type.TIME)
      .put(Types.INTEGER, Field.Type.INTEGER)
      .put(Types.FLOAT, Field.Type.FLOAT)
      .put(Types.DOUBLE, Field.Type.DOUBLE)
      .put(Types.NUMERIC, Field.Type.DECIMAL)
      .put(Types.DECIMAL, Field.Type.DECIMAL)
      .put(Types.BINARY, Field.Type.BYTE_ARRAY)
      .put(Types.VARCHAR, Field.Type.STRING)
      .build();

  private HikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = connectionString;
    bean.useCredentials = true;
    bean.username = () -> username;
    bean.password = () -> password;

    return bean;
  }

  @BeforeClass
  public static void beforeClass() {
    jdbcUtil = UtilsProvider.getJdbcUtil();
  }

  @AfterClass
  public static void afterClass() {
    jdbcUtil = null;
  }

  @Before
  public void setUp() throws SQLException {
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
  }

  @SuppressWarnings("SqlNoDataSourceInspection")
  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP ALL OBJECTS;");
    }

    connection.close();
  }

  @Test
  public void testBadConnectionString() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(BAD_CONNECTION_STRING);
    ProcessorRunner processorRunner = getProcessorRunner(processor);
    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(CANNOT_CONNECT_TO_SPECIFIED_DATABASE));
  }

  @Test
  public void testInitialRecordStructure() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    List<Record> singleRecord = ImmutableList.of(makeRecord(fieldMap1));
    StageRunner.Output output = processorRunner.runProcess(singleRecord);
    Assert.assertEquals(1, output.getRecords().get("lane").size());

    Record record = output.getRecords().get("lane").get(0);
    for (Map.Entry<String, Field>entry : record.get().getValueAsListMap().entrySet()) {
      Assert.assertEquals(fieldMap1.get(entry.getKey()).getRight(), entry.getValue().getValue());
    }

    try (ResultSet metaDataColumns = jdbcUtil.getColumnMetadata(connection, null, tableName)) {
      while (metaDataColumns.next()) {
        Pair<Field.Type, Object> typeAndValue = fieldMap1.get(metaDataColumns.getString(JdbcSchemaReader.COLUMN_NAME).toLowerCase());
        Assert.assertEquals(typeAndValue.getLeft(), typeMap.get(metaDataColumns.getInt(JdbcSchemaReader.DATA_TYPE)));
      }
    }
  }

  @Test
  public void testChangingRecordStructure() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    List<Record> recordList = ImmutableList.of(makeRecord(fieldMap1), makeRecord(fieldMap2));
    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(2, output.getRecords().get("lane").size());

    Record record = output.getRecords().get("lane").get(0);
    for (Map.Entry<String, Field>entry : record.get().getValueAsListMap().entrySet()) {
      Assert.assertEquals(fieldMap1.get(entry.getKey()).getRight(), entry.getValue().getValue());
    }
    record = output.getRecords().get("lane").get(1);
    for (Map.Entry<String, Field>entry : record.get().getValueAsListMap().entrySet()) {
      Assert.assertEquals(fieldMap2.get(entry.getKey()).getRight(), entry.getValue().getValue());
    }

    try (ResultSet metaDataColumns = jdbcUtil.getColumnMetadata(connection, null, tableName)) {
      while (metaDataColumns.next()) {
        Pair<Field.Type, Object> typeAndValue = fieldMap2.get(metaDataColumns.getString(JdbcSchemaReader.COLUMN_NAME).toLowerCase());
        Assert.assertEquals(typeAndValue.getLeft(), typeMap.get(metaDataColumns.getInt(JdbcSchemaReader.DATA_TYPE)));
        if (typeAndValue.getLeft() == Field.Type.DECIMAL) {
          Assert.assertEquals(Integer.parseInt(PRECISION), metaDataColumns.getInt(JdbcSchemaReader.COLUMN_SIZE));
          Assert.assertEquals(Integer.parseInt(SCALE), metaDataColumns.getInt(JdbcSchemaReader.DECIMAL_DIGITS));
        }
      }
    }
  }

  @Test
  public void testIncompatibleRecordStructure() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    List<Record> recordList = ImmutableList.of(makeRecord(fieldMap2), makeRecord(fieldMap3));

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(1, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_303.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testMissingPrecision() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").deleteAttribute("precision");
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_304.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testScaleGreaterThanPrecision() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    Field field = record.get("/column5");
    field.setAttribute("precision", SCALE);
    field.setAttribute("scale", PRECISION);
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_306.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testScaleTooHigh() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").setAttribute("scale", String.valueOf(Integer.MAX_VALUE));
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_305.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testPrecisionTooHigh() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").setAttribute("precision", String.valueOf(Integer.MAX_VALUE));
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_305.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testScaleTooLow() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").setAttribute("scale", "-1");
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_305.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testPrecisionTooLow() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").setAttribute("precision", "-1");
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_305.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testMissingScale() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    Record record = makeRecord(fieldMap2);
    record.get("/column5").deleteAttribute("scale");
    List<Record> recordList = ImmutableList.of(record);

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_304.name(), errorRecord.getHeader().getErrorCode());
  }

  @Test
  public void testWrongRootType() throws Exception {
    JdbcMetadataDProcessor processor = getProcessor(h2ConnectionString);

    ProcessorRunner processorRunner = getProcessorRunner(processor);

    processorRunner.runInit();

    List<Record> recordList = ImmutableList.of(makeListRecord(fieldMap1));

    StageRunner.Output output = processorRunner.runProcess(recordList);
    Assert.assertEquals(0, output.getRecords().get("lane").size());

    Assert.assertEquals(1, processorRunner.getErrorRecords().size());
    Record errorRecord = processorRunner.getErrorRecords().get(0);
    Assert.assertEquals(JdbcErrors.JDBC_300.name(), errorRecord.getHeader().getErrorCode());
  }

  @NotNull
  private JdbcMetadataDProcessor getProcessor(String connectionString) {
    JdbcMetadataDProcessor processor = new JdbcMetadataDProcessor();
    processor.conf = new JdbcMetadataConfigBean();
    processor.conf.hikariConfigBean = createConfigBean(connectionString, username, password);
    processor.conf.decimalDefaultsConfig = new DecimalDefaultsConfig();
    processor.conf.decimalDefaultsConfig.scaleAttribute = HeaderAttributeConstants.ATTR_SCALE;
    processor.conf.decimalDefaultsConfig.precisionAttribute = HeaderAttributeConstants.ATTR_PRECISION;
    processor.conf.tableNameEL = "${record:attribute('table')}";
    return processor;
  }

  private ProcessorRunner getProcessorRunner(JdbcMetadataDProcessor processor) {
    return new ProcessorRunner.Builder(JdbcMetadataDProcessor.class, processor)
          .addOutputLane("lane")
          .setOnRecordError(OnRecordError.TO_ERROR)
          .build();
  }

  @NotNull
  private Record makeRecord(Map<String, Pair<Field.Type, Object>> fieldMap) {
    Record record = RecordCreator.create();
    Record.Header header = record.getHeader();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    for (Map.Entry<String, Pair<Field.Type, Object>> entry : fieldMap.entrySet()) {
      String fieldName = entry.getKey();
      Field.Type fieldType = entry.getValue().getLeft();
      Field field = Field.create(fieldType, entry.getValue().getRight());
      if (fieldType == Field.Type.DECIMAL) {
        field.setAttribute(HeaderAttributeConstants.ATTR_SCALE, SCALE);
        field.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, PRECISION);
      }
      fields.put(fieldName, field);
    }
    record.set(Field.create(fields));
    header.setAttribute("table", tableName);
    return record;
  }

  @NotNull
  private Record makeListRecord(Map<String, Pair<Field.Type, Object>> fieldMap) {
    Record record = RecordCreator.create();
    Record.Header header = record.getHeader();
    ArrayList<Field> fields = new ArrayList<>();
    for (Map.Entry<String, Pair<Field.Type, Object>> entry : fieldMap.entrySet()) {
      String fieldName = entry.getKey();
      Field.Type fieldType = entry.getValue().getLeft();
      Field field = Field.create(fieldType, entry.getValue().getRight());
      if (fieldType == Field.Type.DECIMAL) {
        field.setAttribute(HeaderAttributeConstants.ATTR_SCALE, SCALE);
        field.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, PRECISION);
      }
      fields.add(field);
    }
    record.set(Field.create(fields));
    header.setAttribute("table", tableName);
    return record;
  }
}
