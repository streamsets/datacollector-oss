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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;


public class TableJdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);
  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";

  private static final String JDBC_NAMESPACE_HEADER = "jdbc.";

  private static final String TABLE_QUERY_SELECT = "select * from %s";
  private static final String TABLE_WHERE_CLASS_WITHOUT_QUOTES = " where %s > %s";
  private static final String TABLE_WHERE_CLASS_WITH_QUOTES = " where %s > '%s'";
  private static final String ORDER_BY_CLAUSE = " order by %s";
  private static final String PARTITION_NAME_VALUE = "%s=%s";

  static final String CURRENT_TABLE = "Current Table";
  static final String TABLE_COUNT = "Table Count";
  static final String TABLE_METRICS = "Table Metrics";

  private final HikariPoolConfigBean hikariConfigBean;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final TableJdbcConfigBean tableJdbcConfigBean;
  private final Properties driverProperties = new Properties();
  private final Queue<String> tableQueue;

  private ErrorRecordHandler errorRecordHandler;
  private Map<String, TableContext> orderedTables;
  private Connection connection = null;
  private HikariDataSource hikariDataSource;
  private long lastQueryIntervalTime;

  private final ConcurrentHashMap<String, Object> gaugeMap;

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    tableQueue = new LinkedList<>();
    gaugeMap = new ConcurrentHashMap<>();
  }

  private static String logErrorAndCloseConnection(Connection connection, SQLException e) {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.debug(formattedError, e);
    JdbcUtil.closeQuietly(connection);
    return formattedError;
  }

  private static String serializeOffsetMap(Map<String, String> offsetMap) throws StageException {
    try {
      return OBJECT_MAPPER.writeValueAsString(offsetMap);
    } catch (IOException ex) {
      throw new StageException(JdbcErrors.JDBC_60, ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> deserializeOffsetMap(String lastSourceOffset) throws StageException {
    Map<String, String> offsetMap;
    if (StringUtils.isEmpty(lastSourceOffset)) {
      offsetMap = new HashMap<>();
    } else {
      try {
        offsetMap = OBJECT_MAPPER.readValue(lastSourceOffset, Map.class);
      } catch (IOException ex) {
        throw new StageException(JdbcErrors.JDBC_61, ex);
      }
    }
    return offsetMap;
  }

  private static String buildQuery(TableContext tableContext, String lastOffset) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(
        String.format(
            TABLE_QUERY_SELECT,
            TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName()))
    );
    String offset = (!StringUtils.isEmpty(tableContext.getPartitionStartOffset()))?
        //Use the offset in the configuration
        tableContext.getPartitionStartOffset() :
        // if offset is available
        // get the stored offset (which is of the form partitionName=value) and strip off 'partitionColumn=' prefix
        // else null
        (lastOffset != null)?
            lastOffset.substring(tableContext.getPartitionColumn().length() + 1) : null;

    if (offset != null) {
      //For Char, Varchar, date, time and timestamp embed the value in a quote
      String whereClassTemplate = TableContextUtil.isSqlTypeOneOf(
          tableContext.getPartitionType(),
          Types.CHAR,
          Types.VARCHAR,
          Types.DATE,
          Types.TIME,
          Types.TIMESTAMP
      )? TABLE_WHERE_CLASS_WITH_QUOTES : TABLE_WHERE_CLASS_WITHOUT_QUOTES;
      queryBuilder.append(String.format(whereClassTemplate, tableContext.getPartitionColumn(), offset));
    }

    queryBuilder.append(String.format(ORDER_BY_CLAUSE, tableContext.getPartitionColumn()));
    return queryBuilder.toString();
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
        //TODO:https://issues.streamsets.com/browse/SDC-4281 will introduce the table strategy comparator.
        orderedTables = new TreeMap<>();
        for (TableConfigBean tableConfigBean : tableJdbcConfigBean.tableConfigs) {
          //No duplicates even though a table matches multiple configurations, we will add it only once.
          orderedTables.putAll(TableContextUtil.listTablesForConfig(connection, tableConfigBean));
        }
        if (orderedTables.isEmpty()) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), TableJdbcConfigBean.TABLE_CONFIG, JdbcErrors.JDBC_66));
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
      gaugeMap.put(TABLE_COUNT, orderedTables.size());
      gaugeMap.put(CURRENT_TABLE, "");
      context.createGauge(TABLE_METRICS, new Gauge<Map<String, Object>>() {
        @Override
        public Map<String, Object> getValue() {
          return gaugeMap;
        }
      });
    }
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
    }
    return issues;
  }

  private TableContext getTableContextForCurrentBatch(){
    if (tableQueue.isEmpty()) {
      tableQueue.addAll(orderedTables.keySet());
    }
    return orderedTables.get(tableQueue.poll());
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(maxBatchSize, commonSourceConfigBean.maxBatchSize);
    Map<String, String> offsets = deserializeOffsetMap(lastSourceOffset);

    long delayBeforeQuery = (commonSourceConfigBean.queryInterval * 1000) - (System.currentTimeMillis() - lastQueryIntervalTime);
    ThreadUtil.sleep((lastQueryIntervalTime < 0 || delayBeforeQuery < 0) ? 0 : delayBeforeQuery);

    int recordCount = 0, noOfTablesVisitedInTheCurrentProduce = 0;
    TableContext tableContext = null;
    do {
      try {
        connection = (connection == null) ? hikariDataSource.getConnection() : this.connection;
        tableContext = getTableContextForCurrentBatch();
        String query = buildQuery(tableContext, offsets.get(tableContext.getTableName()));
        ResultSet rs = null;
        try (Statement statement = connection.createStatement()) {
          if (tableJdbcConfigBean.configureFetchSize) {
            statement.setFetchSize(tableJdbcConfigBean.fetchSize);
          }
          //Max rows is set to batch size.
          statement.setMaxRows(commonSourceConfigBean.maxBatchSize);

          rs = statement.executeQuery(query);
          ResultSetMetaData md = rs.getMetaData();

          while (rs.next() && recordCount < batchSize) {
            //TODO: https://issues.streamsets.com/browse/SDC-4280 - Add Max Clob/Blob size action
            LinkedHashMap<String, Field> fields = JdbcUtil.resultSetToFields(
                rs,
                commonSourceConfigBean.maxClobSize,
                commonSourceConfigBean.maxBlobSize,
                errorRecordHandler
            );
            String partitionNameValue =
                String.format(PARTITION_NAME_VALUE, tableContext.getPartitionColumn(), fields.get(tableContext.getPartitionColumn()).getValue());
            Record record = getContext().createRecord(tableContext.getTableName() + ":" + partitionNameValue);
            record.set(Field.createListMap(fields));
            //Set Column Headers
            JdbcUtil.setColumnSpecificHeaders(record, md, JDBC_NAMESPACE_HEADER);

            batchMaker.addRecord(record);
            offsets.put(tableContext.getTableName(), partitionNameValue);
            if (recordCount == 0) {
              gaugeMap.put(
                  CURRENT_TABLE,
                  TableContextUtil.getQualifiedTableName(tableContext.getSchema(), tableContext.getTableName())
              );
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
        errorRecordHandler.onError(JdbcErrors.JDBC_34, buildQuery(tableContext, offsets.get(tableContext.getTableName())), formattedError);
      } finally {
        //Update lastQuery Time
        lastQueryIntervalTime = System.currentTimeMillis();
      }
      noOfTablesVisitedInTheCurrentProduce++;
    } while(recordCount == 0 && noOfTablesVisitedInTheCurrentProduce < orderedTables.size()); //If the current table has no records and if we haven't cycled through all tables.
    return serializeOffsetMap(offsets);
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
