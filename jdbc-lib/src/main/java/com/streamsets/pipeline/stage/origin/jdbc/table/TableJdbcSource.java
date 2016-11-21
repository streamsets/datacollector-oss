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

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.Groups;
import com.streamsets.pipeline.stage.origin.jdbc.table.util.OffsetQueryUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class TableJdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);
  private static final Joiner JOINER = Joiner.on("\n");

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private static final String JDBC_NAMESPACE_HEADER = "jdbc.";

  static final String CURRENT_TABLE = "Current Table";
  static final String TABLE_COUNT = "Table Count";
  static final String TABLE_METRICS = "Table Metrics";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final Properties driverProperties = new Properties();
  private final ConcurrentHashMap<String, Object> gaugeMap;

  private TableOrderProvider tableOrderProvider;
  private ErrorRecordHandler errorRecordHandler;
  private TableJdbcELEvalContext tableJdbcELEvalContext;
  private Calendar calendar;
  private Connection connection = null;
  private HikariDataSource hikariDataSource;
  private long lastQueryIntervalTime;

  public TableJdbcSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean
  ) {
    this.hikariConfigBean = hikariConfigBean;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    lastQueryIntervalTime = -1;
    driverProperties.putAll(hikariConfigBean.driverProperties);
    gaugeMap = new ConcurrentHashMap<>();
  }

  private static String logErrorAndCloseConnection(Connection connection, SQLException e) {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.debug(formattedError, e);
    JdbcUtil.closeQuietly(connection);
    return formattedError;
  }

  private boolean shouldMoveToNextTable(int recordCount, int noOfTablesVisited) {
    return recordCount == 0 && noOfTablesVisited < tableOrderProvider.getNumberOfTables();
  }

  @VisibleForTesting
  void checkConnectionAndBootstrap(Source.Context context, List<ConfigIssue> issues) {
    try {
      hikariDataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
    } catch (StageException e) {
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
    }
    if (issues.isEmpty()) {
      try {
        connection = hikariDataSource.getConnection();

        tableOrderProvider = new TableOrderProviderFactory(connection, tableJdbcConfigBean.tableOrderStrategy).create();

        Map<String, TableContext> allTableContexts = new LinkedHashMap<>();
        for (TableConfigBean tableConfigBean : tableJdbcConfigBean.tableConfigs) {
          //No duplicates even though a table matches multiple configurations, we will add it only once.
          allTableContexts.putAll(TableContextUtil.listTablesForConfig(connection, tableConfigBean));
        }

        LOG.info("Selected Tables: \n {}", JOINER.join(allTableContexts.keySet()));

        try {
          tableOrderProvider.initialize(allTableContexts);
          if (tableOrderProvider.getNumberOfTables() == 0) {
            issues.add(context.createConfigIssue(Groups.JDBC.name(), TableJdbcConfigBean.TABLE_CONFIG, JdbcErrors.JDBC_66));
          }
        } catch (ExecutionException e) {
          LOG.debug("Failure happened when fetching nextTable", e);
          throw new StageException(JdbcErrors.JDBC_67, e);
        }
      } catch (SQLException e) {
        if (connection != null) {
          logErrorAndCloseConnection(connection, e);
          connection = null;
        }
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      } catch (StageException e) {
        if (connection != null) {
          LOG.debug("Error when finding tables:", e);
          JdbcUtil.closeQuietly(connection);
          connection = null;
        }
        issues.add(context.createConfigIssue(Groups.JDBC.name(), TableJdbcConfigBean.TABLE_CONFIG, e.getErrorCode(), e.getParams()));
      }
      gaugeMap.put(TABLE_COUNT, tableOrderProvider.getNumberOfTables());
      gaugeMap.put(CURRENT_TABLE, "");
      context.createGauge(TABLE_METRICS, new Gauge<Map<String, Object>>() {
        @Override
        public Map<String, Object> getValue() {
          return gaugeMap;
        }
      });
    }
  }

  private void initTableEvalContextForProduce(TableContext tableContext) {
    tableJdbcELEvalContext.setCalendar(calendar);
    tableJdbcELEvalContext.setTime(calendar.getTime());
    tableJdbcELEvalContext.setTableContext(tableContext);
  }

  @Override
  protected List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    Source.Context context = getContext();
    errorRecordHandler = new DefaultErrorRecordHandler(context);
    issues = hikariConfigBean.validateConfigs(context, issues);
    issues = commonSourceConfigBean.validateConfigs(context, issues);
    issues = tableJdbcConfigBean.validateConfigs(context, issues, commonSourceConfigBean);
    if (issues.isEmpty()) {
      checkConnectionAndBootstrap(context, issues);
      tableJdbcELEvalContext = new TableJdbcELEvalContext(getContext(), getContext().createELVars());
      calendar = Calendar.getInstance(TimeZone.getTimeZone(tableJdbcConfigBean.timeZoneID));
    }
    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(maxBatchSize, commonSourceConfigBean.maxBatchSize);
    Map<String, String> offsets = OffsetQueryUtil.deserializeOffsetMap(lastSourceOffset);

    long delayBeforeQuery = (commonSourceConfigBean.queryInterval * 1000) - (System.currentTimeMillis() - lastQueryIntervalTime);
    ThreadUtil.sleep((lastQueryIntervalTime < 0 || delayBeforeQuery < 0) ? 0 : delayBeforeQuery);

    int recordCount = 0, noOfTablesVisited = 0;
    do {
      String query = null;
      try {
        connection = (connection == null) ? hikariDataSource.getConnection() : this.connection;
        TableContext tableContext = tableOrderProvider.nextTable();

        initTableEvalContextForProduce(tableContext);

        query = OffsetQueryUtil.buildQuery(tableContext, offsets.get(tableContext.getTableName()), tableJdbcELEvalContext);

        //Clear the initial offset after the  query is build so we will not use the initial offset from the next
        //time the table is used.
        tableContext.clearStartOffset();
        ResultSet rs = null;
        try (Statement statement = connection.createStatement()) {
          if (tableJdbcConfigBean.configureFetchSize) {
            statement.setFetchSize(tableJdbcConfigBean.fetchSize);
          }
          //Max rows is set to batch size.
          statement.setMaxRows(commonSourceConfigBean.maxBatchSize);

          LOG.info("Executing Query :{}", query);
          rs = statement.executeQuery(query);
          ResultSetMetaData md = rs.getMetaData();
          while (rs.next() && recordCount < batchSize) {
            LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
                rs,
                commonSourceConfigBean.maxClobSize,
                commonSourceConfigBean.maxBlobSize,
                errorRecordHandler
            );

            String offsetFormat = OffsetQueryUtil.getOffsetFormatForPartitionColumns(tableContext, fields);
            Record record = getContext().createRecord(tableContext.getTableName() + ":" + offsetFormat);
            record.set(Field.createListMap(fields));

            //Set Column Headers
            JdbcUtil.setColumnSpecificHeaders(record, md, JDBC_NAMESPACE_HEADER);

            batchMaker.addRecord(record);
            offsets.put(tableContext.getTableName(), offsetFormat);
            if (recordCount == 0) {
              String qualifiedTableName = TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName());
              gaugeMap.put(CURRENT_TABLE, qualifiedTableName);
              LOG.info("Generating records from table : {}", qualifiedTableName);
            }
            recordCount++;
          }
        } finally {
          if (rs != null) {
            JdbcUtil.closeQuietly(rs);
          }
        }
      } catch (SQLException e) {
        String formattedError = logErrorAndCloseConnection(connection, e);
        connection = null;
        LOG.debug("Query failed at: {}", lastQueryIntervalTime);
        //Throw Stage Errors
        errorRecordHandler.onError(JdbcErrors.JDBC_34, query, formattedError);
      } catch (ExecutionException e) {
        LOG.debug("Failure happened when fetching nextTable", e);
        errorRecordHandler.onError(JdbcErrors.JDBC_67, e);
      } finally {
        //Update lastQuery Time
        lastQueryIntervalTime = System.currentTimeMillis();
      }
      noOfTablesVisited++;
    } while(shouldMoveToNextTable(recordCount, noOfTablesVisited)); //If the current table has no records and if we haven't cycled through all tables.
    return OffsetQueryUtil.serializeOffsetMap(offsets);
  }

  @Override
  public void destroy() {
    if (connection != null) {
      JdbcUtil.closeQuietly(connection);
    }
    if (hikariDataSource != null) {
      JdbcUtil.closeQuietly(hikariDataSource);
    }
  }
}
