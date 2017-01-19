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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class TableJdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);
  private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");

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

  private ErrorRecordHandler errorRecordHandler;
  private TableOrderProvider tableOrderProvider;
  private TableJdbcELEvalContext tableJdbcELEvalContext;
  private TableContext tableContext;
  private Calendar calendar;
  private long lastQueryIntervalTime;

  private HikariDataSource hikariDataSource;
  private Connection connection = null;
  private String query = null;
  private Statement st = null;
  private ResultSet rs = null;

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

  private static String logError(SQLException e) {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.debug(formattedError, e);
    return formattedError;
  }

  private boolean shouldMoveToNextTable(int recordCount, int noOfTablesVisited) {
    return recordCount == 0 && noOfTablesVisited < tableOrderProvider.getNumberOfTables();
  }

  private void initGauge(Source.Context context) {
    gaugeMap.put(TABLE_COUNT, tableOrderProvider.getNumberOfTables());
    gaugeMap.put(CURRENT_TABLE, "");
    context.createGauge(TABLE_METRICS, new Gauge<Map<String, Object>>() {
      @Override
      public Map<String, Object> getValue() {
        return gaugeMap;
      }
    });
  }

  private void updateGauge() {
    String qualifiedTableName = TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName());
    gaugeMap.put(CURRENT_TABLE, qualifiedTableName);
    LOG.info("Generating records from table : {}", qualifiedTableName);
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

        LOG.info("Selected Tables: \n {}", NEW_LINE_JOINER.join(allTableContexts.keySet()));

        try {
          tableOrderProvider.initialize(allTableContexts);
          if (tableOrderProvider.getNumberOfTables() == 0) {
            issues.add(
                context.createConfigIssue(
                    Groups.TABLE.name(),
                    TableJdbcConfigBean.TABLE_CONFIG,
                    JdbcErrors.JDBC_66
                )
            );
          }
        } catch (ExecutionException e) {
          LOG.debug("Failure happened when fetching nextTable", e);
          throw new StageException(JdbcErrors.JDBC_67, e);
        }
        initGauge(context);
      } catch (SQLException e) {
        logError(e);
        closeConnection();
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      } catch (StageException e) {
        LOG.debug("Error when finding tables:", e);
        closeConnection();
        issues.add(context.createConfigIssue(Groups.JDBC.name(), TableJdbcConfigBean.TABLE_CONFIG, e.getErrorCode(), e.getParams()));
      }
    }
  }


  private void initTableEvalContextForProduce(TableContext tableContext) {
    tableJdbcELEvalContext.setCalendar(calendar);
    tableJdbcELEvalContext.setTime(calendar.getTime());
    tableJdbcELEvalContext.setTableContext(tableContext);
  }

  private void reinitializeContext(Map<String, String> offsets) throws SQLException, ExecutionException, StageException {
    //We just read what we need from the last table, time to get the next table.
    tableContext = tableOrderProvider.nextTable();

    initTableEvalContextForProduce(tableContext);

    query = OffsetQueryUtil.buildQuery(tableContext, offsets.get(tableContext.getTableName()), tableJdbcELEvalContext);

    //Clear the initial offset after the  query is build so we will not use the initial offset from the next
    //time the table is used.
    tableContext.clearStartOffset();

    st = connection.createStatement();

    if (tableJdbcConfigBean.configureFetchSize) {
      st.setFetchSize(tableJdbcConfigBean.fetchSize);
    }
    if (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) {
      //Max rows is set to batch size only when we switch tables for each batch.
      st.setMaxRows(commonSourceConfigBean.maxBatchSize);
    }

    LOG.info("Executing Query :{}", query);

    rs = st.executeQuery(query);
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
      try {
        connection = (connection == null) ? hikariDataSource.getConnection() : this.connection;

        //Meaning we will have to switch tables, execute a new query and get records.
        if (rs == null) {
          reinitializeContext(offsets);
        }

        ResultSetMetaData md = rs.getMetaData();
        boolean shouldCloseRs = false;

        try {
          while (recordCount < batchSize) {
            //Close ResultSet if there are no more rows
            if (!rs.next()) {
              shouldCloseRs = true;
              break;
            }
            LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
                rs,
                commonSourceConfigBean.maxClobSize,
                commonSourceConfigBean.maxBlobSize,
                errorRecordHandler
            );

            String offsetFormat = OffsetQueryUtil.getOffsetFormatFromColumns(tableContext, fields);
            Record record = getContext().createRecord(tableContext.getTableName() + ":" + offsetFormat);
            record.set(Field.createListMap(fields));

            //Set Column Headers
            JdbcUtil.setColumnSpecificHeaders(record, md, JDBC_NAMESPACE_HEADER);

            batchMaker.addRecord(record);

            offsets.put(tableContext.getTableName(), offsetFormat);

            if (recordCount == 0) {
              updateGauge();
            }
            recordCount++;
          }
          //We close the result set definitely if the SWITCH_TABLES is the BatchStrategy
          shouldCloseRs = (tableJdbcConfigBean.batchTableStrategy == BatchTableStrategy.SWITCH_TABLES) || shouldCloseRs;
        } finally {
          //Make sure we close the result set only when there are no more rows in the result set
          //This will happen if Batch Strategy is SWITCH_TABLES
          //or We use PROCESS_ALL_ROWS and there are no more rows to process in the current table.
          if (shouldCloseRs) {
            LOG.debug(
                "Closing the current result set for: {}",
                TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName())
            );
            JdbcUtil.closeQuietly(rs);
            JdbcUtil.closeQuietly(st);
            rs = null;
            st = null;
          }
        }
      } catch (SQLException e) {
        String formattedError = logError(e);
        closeConnection();
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
    closeConnection();
    JdbcUtil.closeQuietly(hikariDataSource);
  }

  private void closeConnection() {
    JdbcUtil.closeQuietly(rs);
    JdbcUtil.closeQuietly(st);
    JdbcUtil.closeQuietly(connection);
    st = null;
    rs = null;
    connection = null;
    query = null;
  }
}
