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
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.DataType;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.BatchTableStrategy;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Ignore //SCJDBC-131. Migrate this class to STF
public class TestTableJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableJdbcSource.class);

  private static final String USER_NAME = "sa";
  private static final String PASSWORD = "sa";
  private static final String database = "TEST";
  private static final String JDBC_URL = "jdbc:h2:mem:" + database;

  private void testWrongConfiguration(TableJdbcSource tableJdbcSource, boolean isMockNeeded) throws Exception {
    if (isMockNeeded) {
      tableJdbcSource = Mockito.spy(tableJdbcSource);
      Mockito.doNothing().when(tableJdbcSource).checkConnectionAndBootstrap(
          Mockito.any(PushSource.Context.class), Mockito.anyListOf(Stage.ConfigIssue.class)
      );
    }
    PushSourceRunner runner = new PushSourceRunner.Builder(TableJdbcDSource.class, tableJdbcSource)
        .addOutputLane("a").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testNoTableConfiguration() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder(JDBC_URL, true, USER_NAME, PASSWORD)
        .tableConfigBeans(Collections.emptyList())
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testWrongSqlConnectionConfig() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder().tablePattern("testTable").build()
            )
        )
        .build();
    testWrongConfiguration(tableJdbcSource, false);
  }

  @Test
  public void testDefaultPartitioningMode() throws Exception {
    final TableConfigBean tableConfig = new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder().tablePattern(
        "testTable"
    ).build();
    assertThat(tableConfig.getPartitioningMode(), equalTo(PartitioningMode.DISABLED));
  }

  @Test
  public void testLessConnectionPoolSizeThanNoOfThreads() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder().tablePattern("testTable").build()
            )
        )
        .numberOfThreads(3)
        .maximumPoolSize(2)
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testWrongBatchesFromResultSetConfig() throws Exception {
    TableJdbcSource tableJdbcSource = new TableJdbcSourceTestBuilder( "jdbc:db://localhost:1000", true, USER_NAME, PASSWORD)
        .tableConfigBeans(
            ImmutableList.of(
                new TableJdbcSourceTestBuilder.TableConfigBeanTestBuilder()
                    .tablePattern("testTable")
                    .build()
            )
        )
        .numberOfBatchesFromResultset(0)
        .batchTableStrategy(BatchTableStrategy.SWITCH_TABLES)
        .build();
    testWrongConfiguration(tableJdbcSource, true);
  }

  @Test
  public void testTimeBasedOffsetValues() throws SQLException, StageException {
    final String tableName = "myMock";
    final int nanos = 987654321;
    final long partitionSize = 1000 * 60 * 60 * 24 + 19742;
    final String partitionSizeStr = String.valueOf(partitionSize);

    MockResultSet rs = new MockResultSet(tableName);

    LocalDateTime baseDate = LocalDateTime.of(2017, 8, 16,
        13,
        2,
        17,
        nanos
    );

    final Date dateVal = new Date(baseDate.toLocalDate().atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000);
    final Time timeVal = new Time(baseDate.toLocalTime().toNanoOfDay() / JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT);
    final Timestamp tsValWithNanos = Timestamp.from(baseDate.toInstant(ZoneOffset.UTC));
    final Instant baseDateNoNanos = baseDate.toInstant(ZoneOffset.UTC).minusNanos(
        nanos % JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT
    );
    final Timestamp tsValNoNanos = Timestamp.from(baseDateNoNanos);

    final String timestampColWithNanos = "timestamp_col_with_nanos";
    final String dateCol = "date_col";
    final String timeCol = "time_col";
    final String timestampColNoNanos = "timestamp_col";

    rs.addColumn(timestampColWithNanos, new Timestamp[] {tsValWithNanos});
    rs.addColumn(dateCol, new Date[] {dateVal});
    rs.addColumn(timeCol, new Time[] {timeVal});
    rs.addColumn(timestampColNoNanos, new Timestamp[] {tsValNoNanos});

    final MockResultSetMetaData resultSetMetaData = new MockResultSetMetaData();
    resultSetMetaData.setColumnType(1, Types.TIMESTAMP);
    resultSetMetaData.setColumnType(2, Types.DATE);
    resultSetMetaData.setColumnType(3, Types.TIME);
    resultSetMetaData.setColumnType(4, Types.TIMESTAMP);
    resultSetMetaData.setColumnName(1, timestampColWithNanos);
    resultSetMetaData.setColumnName(2, dateCol);
    resultSetMetaData.setColumnName(3, timeCol);
    resultSetMetaData.setColumnName(4, timestampColNoNanos);
    resultSetMetaData.setColumnCount(4);

    rs.setResultSetMetaData(resultSetMetaData);
    try {
      assertTrue(rs.next());
    }
    catch (SQLException e) {
      LOG.error("SQLException attempting to advance MockResultSet", e);
      Assert.fail(String.format("Failed to advance MockResultSet: %s", e.getMessage()));
    }

    final HashMap<String, DataType> columnsToTypes = new HashMap<>();
    columnsToTypes.put(timestampColWithNanos, DataType.USE_COLUMN_TYPE);
    columnsToTypes.put(dateCol, DataType.USE_COLUMN_TYPE);
    columnsToTypes.put(timeCol, DataType.USE_COLUMN_TYPE);

    LinkedHashMap<String, Field> fields = UtilsProvider.getJdbcUtil().resultSetToFields(rs, 0, 0,
        columnsToTypes, new FailTestErrorRecordHandler(), UnknownTypeAction.CONVERT_TO_STRING, DatabaseVendor.UNKNOWN);

    assertThat(fields, hasKey(timestampColWithNanos));
    assertThat(fields.get(timestampColWithNanos).getAttributeNames(), contains(JdbcUtil.FIELD_ATTRIBUTE_NANOSECONDS));
    assertThat(fields.get(timestampColWithNanos).getAttribute(JdbcUtil.FIELD_ATTRIBUTE_NANOSECONDS), equalTo(
        String.valueOf(nanos % JdbcUtil.NANOS_TO_MILLIS_ADJUSTMENT)
    ));

    final Map<String, String> nextOffsets = new HashMap<>();

    final Map<String, String> tsWithNanosOffsets = getOffsetValuesForColumn(
        tableName,
        timestampColWithNanos,
        Types.TIMESTAMP,
        partitionSizeStr,
        fields,
        nextOffsets
    );

    assertThat(tsWithNanosOffsets, hasKey(timestampColWithNanos));
    final String expectedTsWithNanosOffset = TableContextUtil.getOffsetValueForTimestampParts(baseDate.toInstant(
        ZoneOffset.UTC
    ).toEpochMilli(), baseDate.getNano());
    assertThat(tsWithNanosOffsets.get(timestampColWithNanos), equalTo(expectedTsWithNanosOffset));
    assertThat(TableContextUtil.getTimestampForOffsetValue(expectedTsWithNanosOffset), equalTo(tsValWithNanos));
    assertThat(nextOffsets, hasKey(timestampColWithNanos));
    final String nextOffsetTsWithNanos = nextOffsets.get(timestampColWithNanos);
    final Timestamp nextTsOffsetWithNanos = Timestamp.from(baseDate.toInstant(ZoneOffset.UTC).plusMillis(partitionSize));
    final Timestamp nextTsValWithNanos = TableContextUtil.getTimestampForOffsetValue(nextOffsetTsWithNanos);
    assertThat(nextTsValWithNanos, equalTo(nextTsOffsetWithNanos));
    assertThat(nextTsValWithNanos.getTime() - tsValWithNanos.getTime(), equalTo(partitionSize));

    final Map<String, String> tsNoNanosOffsets = getOffsetValuesForColumn(
        tableName,
        timestampColNoNanos,
        Types.TIMESTAMP,
        partitionSizeStr,
        fields,
        nextOffsets
    );

    assertThat(tsNoNanosOffsets, hasKey(timestampColNoNanos));
    final String expectedTsNoNanosOffset = TableContextUtil.getOffsetValueForTimestampParts(baseDate.toInstant(
        ZoneOffset.UTC
    ).toEpochMilli(), 0);
    assertThat(tsNoNanosOffsets.get(timestampColNoNanos), equalTo(expectedTsNoNanosOffset));
    assertThat(TableContextUtil.getTimestampForOffsetValue(expectedTsNoNanosOffset), equalTo(tsValNoNanos));
    assertThat(nextOffsets, hasKey(timestampColNoNanos));
    final String nextOffsetTsNoNanos = nextOffsets.get(timestampColNoNanos);
    final Timestamp expectedTsOffsetNoNanos = Timestamp.from(baseDateNoNanos.plusMillis(partitionSize));
    final Timestamp actualNextTsValNoNanos = TableContextUtil.getTimestampForOffsetValue(nextOffsetTsNoNanos);
    assertThat(actualNextTsValNoNanos, equalTo(expectedTsOffsetNoNanos));
    assertThat(actualNextTsValNoNanos.getTime() - tsValNoNanos.getTime(), equalTo(partitionSize));

    final Map<String, String> dateOffsets = getOffsetValuesForColumn(
        tableName,
        dateCol,
        Types.DATE,
        partitionSizeStr,
        fields,
        nextOffsets
    );

    assertThat(dateOffsets, hasKey(dateCol));
    assertThat(dateOffsets.get(dateCol), equalTo(String.valueOf(dateVal.getTime())));
    assertThat(nextOffsets, hasKey(dateCol));
    final String nextOffsetDate = nextOffsets.get(dateCol);
    final Date nextDateVal = new Date(Long.parseLong(nextOffsetDate));
    assertThat(nextDateVal.getTime() - dateVal.getTime(), equalTo(partitionSize));

    final Map<String, String> timeOffsets = getOffsetValuesForColumn(
        tableName,
        timeCol,
        Types.TIME,
        partitionSizeStr,
        fields,
        nextOffsets
    );

    assertThat(timeOffsets, hasKey(timeCol));
    assertThat(timeOffsets.get(timeCol), equalTo(String.valueOf(timeVal.getTime())));
    assertThat(nextOffsets, hasKey(timeCol));
    final String nextOffsetTime = nextOffsets.get(timeCol);
    final Time nextTimeVal = new Time(Long.parseLong(nextOffsetTime));
    assertThat(nextTimeVal.getTime() - timeVal.getTime(), equalTo(partitionSize));
  }

  private static Map<String, String> getOffsetValuesForColumn(
      String tableName,
      String offsetCol,
      int offsetColType,
      String partitionSize,
      LinkedHashMap<String, Field> fields,
      Map<String, String> nextPartitionOffsetsToCalculate
  ) throws StageException {
    nextPartitionOffsetsToCalculate.clear();
    final LinkedHashMap<String, Integer> offsetColumnToType = new LinkedHashMap<>();
    offsetColumnToType.put(offsetCol, offsetColType);

    final Map<String, String> offsetColumnToStartOffset = Collections.emptyMap();
    final Map<String, String> offsetColumnToPartitionOffsetAdjustments = new HashMap<>();
    offsetColumnToPartitionOffsetAdjustments.put(offsetCol, partitionSize);
    final Map<String, String> offsetColumnToMinValues = Collections.emptyMap();

    final TableContext tableContext = new TableContext(
        DatabaseVendor.UNKNOWN,
        QuoteChar.NONE,
        "",
        tableName,
        offsetColumnToType,
        offsetColumnToStartOffset,
        offsetColumnToPartitionOffsetAdjustments,
        offsetColumnToMinValues,
        Collections.emptyMap(),
        TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE,
        PartitioningMode.REQUIRED,
        -1,
        null
    );

    final TableRuntimeContext tableRuntimeContext = new TableRuntimeContext(
        tableContext,
        false,
        true,
        1,
        Collections.emptyMap(),
        Collections.emptyMap()
    );


    final Map<String, String> offsets = OffsetQueryUtil.getOffsetsFromColumns(tableRuntimeContext, fields);

    offsets.forEach((col, offset) -> {
      final String nextOffset = TableContextUtil.generateNextPartitionOffset(tableContext, col, offset);
      nextPartitionOffsetsToCalculate.put(col, nextOffset);
    });

    return offsets;
  }

  private static class FailTestErrorRecordHandler implements ErrorRecordHandler {
    @Override
    public void onError(ErrorCode errorCode, Object... params) throws StageException {
      fail();
    }

    @Override
    public void onError(OnRecordErrorException error) throws StageException {
      fail();
    }

    @Override
    public void onError(List<Record> batch, StageException error) throws StageException {
      fail();
    }

    private static void fail() {
      Assert.fail("Should not be called");
    }
  }
}
