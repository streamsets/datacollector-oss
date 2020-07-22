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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.MSOperationCode;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.service.sshtunnel.DummySshService;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class JdbcSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  public static final String QUERY = "query";
  public static final String TIMESTAMP = "timestamp";
  public static final String ERROR = "error";
  public static final String ROW_COUNT = "rows";
  public static final String SOURCE_OFFSET = "offset";
  private static final String OFFSET_COLUMN = "offsetColumn";
  private static final String INITIAL_OFFSET = "initialOffset";
  private static final String QUERY_INTERVAL_EL = "queryInterval";
  private static final String TXN_ID_COLUMN_NAME = "txnIdColumnName";
  private static final String TXN_MAX_SIZE = "txnMaxSize";
  private static final String JDBC_NS_HEADER_PREFIX = "jdbcNsHeaderPrefix";
  private static final HashFunction HF = Hashing.sha256();

  private final boolean isIncrementalMode;
  private final String query;
  private final String initialOffset;
  private final String offsetColumn;
  private final String txnColumnName;
  private final int txnMaxSize;
  private final JdbcRecordType jdbcRecordType;
  private final CommonSourceConfigBean commonSourceConfigBean;
  private final HikariPoolConfigBean hikariConfigBean;
  private final boolean createJDBCNsHeaders;
  private final String jdbcNsHeaderPrefix;
  private final boolean disableValidation;
  private final UnknownTypeAction unknownTypeAction;

  private ErrorRecordHandler errorRecordHandler;
  private long queryIntervalMillis = Long.MIN_VALUE;
  private HikariDataSource dataSource = null;
  private Connection connection = null;
  private ResultSet resultSet = null;
  private long lastQueryCompletedTime = 0L;
  private String preparedQuery;
  private int queryRowCount = 0;
  private int numQueryErrors = 0;
  private SQLException firstQueryException = null;
  private long noMoreDataRecordCount = 0;
  private String tableNames;
  private boolean shouldFire = true;
  private boolean firstTime = true;

  private final JdbcUtil jdbcUtil;
  private SshTunnelService sshTunnelService;

  private boolean checkBatchSize = true;

  public JdbcSource(
      boolean isIncrementalMode,
      String query,
      String initialOffset,
      String offsetColumn,
      boolean disableValidation,
      String txnColumnName,
      int txnMaxSize,
      JdbcRecordType jdbcRecordType,
      CommonSourceConfigBean commonSourceConfigBean,
      boolean createJDBCNsHeaders,
      String jdbcNsHeaderPrefix,
      HikariPoolConfigBean hikariConfigBean,
      UnknownTypeAction unknownTypeAction,
      long queryInterval
  ) {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.isIncrementalMode = isIncrementalMode;
    this.query = query;
    this.initialOffset = initialOffset;
    this.offsetColumn = offsetColumn;
    this.disableValidation = disableValidation;
    this.queryIntervalMillis = 1000 * queryInterval;
    this.txnColumnName = txnColumnName;
    this.txnMaxSize = txnMaxSize;
    this.commonSourceConfigBean = commonSourceConfigBean;
    this.jdbcRecordType = jdbcRecordType;
    this.hikariConfigBean = hikariConfigBean;
    this.createJDBCNsHeaders = createJDBCNsHeaders;
    this.jdbcNsHeaderPrefix = jdbcNsHeaderPrefix;
    this.unknownTypeAction = unknownTypeAction;
  }

  protected BasicConnectionString getBasicConnectionString() {
    return new BasicConnectionString(hikariConfigBean.getPatterns(), hikariConfigBean.getConnectionStringTemplate());
  }

  @Override
  protected List<ConfigIssue> init() {
    if (disableValidation) {
      LOG.warn("JDBC Origin initialized with Validation Disabled.");
    }

    List<ConfigIssue> issues = new ArrayList<>();
    Source.Context context = getContext();

    sshTunnelService = getSSHService();

    BasicConnectionString.Info
        info
        = getBasicConnectionString().getBasicConnectionInfo(hikariConfigBean.getConnectionString());

    if (info != null) {
      // basic connection string format
      SshTunnelService.HostPort target = new SshTunnelService.HostPort(info.getHost(), info.getPort());
      Map<SshTunnelService.HostPort, SshTunnelService.HostPort>
          portMapping
          = sshTunnelService.start(Collections.singletonList(target));
      SshTunnelService.HostPort tunnel = portMapping.get(target);
      info = info.changeHostPort(tunnel.getHost(), tunnel.getPort());
      hikariConfigBean.setConnectionString(getBasicConnectionString().getBasicConnectionUrl(info));
    } else {
      // complex connection string format, we don't support this right now with SSH tunneling
      issues.add(getContext().createConfigIssue("JDBC",
          "hikariConfigBean.connectionString",
          hikariConfigBean.getNonBasicUrlErrorCode()
      ));
    }

    errorRecordHandler = new DefaultErrorRecordHandler(context);
    issues = hikariConfigBean.validateConfigs(context, issues);

    if (queryIntervalMillis < 0) {
      issues.add(getContext().createConfigIssue(Groups.JDBC.name(), QUERY_INTERVAL_EL, JdbcErrors.JDBC_27));
    }

    issues = commonSourceConfigBean.validateConfigs(context, issues);

    // Incremental mode have special requirements for the query form
    if (isIncrementalMode) {
      if (StringUtils.isEmpty(offsetColumn)) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), OFFSET_COLUMN, JdbcErrors.JDBC_51, "Can't be empty"));
      }
      if (StringUtils.isEmpty(initialOffset)) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), INITIAL_OFFSET, JdbcErrors.JDBC_51, "Can't be empty"));
      }

      final String formattedOffsetColumn = Pattern.quote(offsetColumn.toUpperCase());
      Pattern offsetColumnInWhereAndOrderByClause = Pattern.compile(String.format("(?s).*\\bWHERE\\b.*(\\b%s\\b)" +
              ".*\\bORDER BY\\b.*\\b%s\\b.*",
          formattedOffsetColumn,
          formattedOffsetColumn
      ));

      if (!disableValidation) {
        String upperCaseQuery = query.toUpperCase();
        boolean checkOffsetColumnInWhereOrder = true;
        if (!upperCaseQuery.contains("WHERE")) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, JdbcErrors.JDBC_38, "WHERE"));
          checkOffsetColumnInWhereOrder = false;
        }
        if (!upperCaseQuery.contains("ORDER BY")) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, JdbcErrors.JDBC_38, "ORDER BY"));
          checkOffsetColumnInWhereOrder = false;
        }
        if (checkOffsetColumnInWhereOrder && !offsetColumnInWhereAndOrderByClause.matcher(upperCaseQuery).matches()) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, JdbcErrors.JDBC_29, offsetColumn));
        }
      }
    }

    if (txnMaxSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), TXN_MAX_SIZE, JdbcErrors.JDBC_10, txnMaxSize, 0));
    }

    if (createJDBCNsHeaders && !jdbcNsHeaderPrefix.endsWith(".")) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), JDBC_NS_HEADER_PREFIX, JdbcErrors.JDBC_15));
    }

    Properties driverProps = new Properties();
    try {
      driverProps = hikariConfigBean.getDriverProperties();
      if (null == dataSource) {
        dataSource = jdbcUtil.createDataSourceForRead(hikariConfigBean);
      }
    } catch (StageException e) {
      LOG.error(JdbcErrors.JDBC_00.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, e.getErrorCode(), e.getParams()));
    }

    // Don't proceed with validation query if there are issues or if validation is disabled
    if (!issues.isEmpty() || disableValidation) {
      return issues;
    }

    if(!disableValidation) {
      try (Connection validationConnection = dataSource.getConnection()) { // NOSONAR
        DatabaseMetaData dbMetadata = validationConnection.getMetaData();
        // If CDC is enabled, scrollable cursors must be supported by JDBC driver.
        supportsScrollableCursor(issues, context, dbMetadata);
        try (Statement statement = validationConnection.createStatement()) {
          statement.setFetchSize(1);
          statement.setMaxRows(1);
          final String preparedQuery = prepareQuery(query, initialOffset);
          executeValidationQuery(issues, context, statement, preparedQuery);
        }
      } catch (SQLException e) {
        String formattedError = jdbcUtil.formatSqlException(e);
        LOG.error(formattedError);
        LOG.debug(formattedError, e);
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, formattedError));
      }
    }

    if(issues.isEmpty()) {
      LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
      // TODO: add the per-event specific details here.
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, query);
      event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.JDBC.name());
      Map<String, String> props = new HashMap<>();
      props.put("Connection String", hikariConfigBean.getConnectionString());
      props.put("Offset Column", offsetColumn);
      props.put("Is Incremental Mode", isIncrementalMode ? "true" : "false");
      if (!StringUtils.isEmpty(tableNames)) {
        event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME,
            hikariConfigBean.getConnectionString() + " " + tableNames
        );
        props.put("Table Names", tableNames);

      } else {
        event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, hikariConfigBean.getConnectionString());
      }

      for (final String n : driverProps.stringPropertyNames()) {
        props.put(n, driverProps.getProperty(n));
      }
      event.setProperties(props);
      getContext().publishLineageEvent(event);
    }

    shouldFire = true;
    firstTime = true;

    return issues;
  }

  private SshTunnelService getSSHService() {
    SshTunnelService declaredSshTunnelService;
    try {
      declaredSshTunnelService = getContext().getService(SshTunnelService.class);
    } catch (RuntimeException e) {
      declaredSshTunnelService = new DummySshService();
    }
    return declaredSshTunnelService;
  }

  private void supportsScrollableCursor(
      List<ConfigIssue> issues, Source.Context context, DatabaseMetaData dbMetadata
  ) throws SQLException {
    if (!txnColumnName.isEmpty() && !dbMetadata.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
      issues.add(context.createConfigIssue(Groups.CDC.name(), TXN_ID_COLUMN_NAME, JdbcErrors.JDBC_30));
    }
  }

  private void executeValidationQuery(
      List<ConfigIssue> issues, Source.Context context, Statement statement, String preparedQuery
  ) {
    try (ResultSet rs = statement.executeQuery(preparedQuery)) {
      validateResultSetMetadata(issues, context, rs);
    } catch (SQLException e) {
      String formattedError = jdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);
      issues.add(
          context.createConfigIssue(Groups.JDBC.name(), QUERY, JdbcErrors.JDBC_34, preparedQuery, formattedError)
      );
    }
  }

  private void validateResultSetMetadata(List<ConfigIssue> issues, Source.Context context, ResultSet rs) {
    Set<String> allTables = new HashSet<>();
    try {
      Set<String> columnLabels = new HashSet<>();
      ResultSetMetaData metadata = rs.getMetaData();
      int columnIdx = metadata.getColumnCount() + 1;
      while (--columnIdx > 0) {
        String columnLabel = metadata.getColumnLabel(columnIdx);
        if (columnLabels.contains(columnLabel)) {
          issues.add(context.createConfigIssue(Groups.JDBC.name(), QUERY, JdbcErrors.JDBC_31, columnLabel));
        } else {
          columnLabels.add(columnLabel);
        }
        allTables.add(metadata.getTableName(columnIdx));
      }
      if (!StringUtils.isEmpty(offsetColumn) && offsetColumn.contains(".")) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), OFFSET_COLUMN, JdbcErrors.JDBC_32, offsetColumn));
      } else {
        rs.findColumn(offsetColumn);
      }
    } catch (SQLException e) {
      // Log a warning instead of an error because some implementations such as Oracle have implicit
      // "columns" such as ROWNUM that won't appear as part of the result set.
      LOG.warn(JdbcErrors.JDBC_33.getMessage(), offsetColumn, query);
      LOG.warn(jdbcUtil.formatSqlException(e));
    }
    tableNames = StringUtils.join(allTables, ", ");
  }

  @Override
  public void destroy() {
    closeQuietly(resultSet);
    closeQuietly(connection);
    closeQuietly(dataSource);
    if (sshTunnelService != null){
      sshTunnelService.stop();
    }
    super.destroy();
  }

  protected String getPreparedQuery() {
    return preparedQuery;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) {
    int batchSize = Math.min(this.commonSourceConfigBean.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && commonSourceConfigBean.maxBatchSize > maxBatchSize) {
      getContext().reportError(JdbcErrors.JDBC_502, maxBatchSize);
      checkBatchSize = false;
    }

    String nextSourceOffset = lastSourceOffset == null ? initialOffset : lastSourceOffset;

    long now = System.currentTimeMillis();
    long delay = Math.max(0, (lastQueryCompletedTime + queryIntervalMillis) - now);

    if (delay > 0) {
      // Sleep in one second increments so we don't tie up the app.
      LOG.debug("{}ms remaining until next fetch.", delay);
      ThreadUtil.sleep(Math.min(delay, 1000));
    } else {
      Statement statement = null;
      sshTunnelService.healthCheck();
      Hasher hasher = HF.newHasher();
      try {
        if (null == resultSet || resultSet.isClosed()) {
          // The result set got closed outside of us, so we also clean up the connection (if any)
          closeQuietly(connection);
          connection = getProduceConnection();

          preparedQuery = prepareQuery(query, lastSourceOffset);
          if (!txnColumnName.isEmpty()) {
            // CDC requires scrollable cursors.
            statement = getStatement(connection, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          } else {
            statement = getStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          }

          int fetchSize = batchSize;
          // MySQL does not support cursors or fetch size except 0 and "streaming" (1 at a time).
          if (hikariConfigBean.getConnectionString().toLowerCase().contains("mysql")) {
            // Enable MySQL streaming mode.
            fetchSize = Integer.MIN_VALUE;
          }
          LOG.debug("Using query fetch size: {}", fetchSize);
          statement.setFetchSize(fetchSize);

          if (getContext().isPreview()) {
            statement.setMaxRows(batchSize);
          }

          LOG.trace("Executing query: {}", preparedQuery);
          String hashedQuery = hasher.putString(preparedQuery, Charsets.UTF_8).hash().toString();
          LOG.debug("Executing query: {}", hashedQuery);
          resultSet = executeQuery(statement, preparedQuery);
          queryRowCount = 0;
          numQueryErrors = 0;
          firstQueryException = null;
        }

        // Read Data and track last offset
        int rowCount = 0;
        String lastTransactionId = "";
        boolean haveNext = true;
        while (continueReading(rowCount, batchSize) && (haveNext = resultSet.next())) {
          final Record record = processRow(resultSet, rowCount);

          if (null != record) {
            if (!txnColumnName.isEmpty()) {
              String newTransactionId = resultSet.getString(txnColumnName);
              if (lastTransactionId.isEmpty()) {
                lastTransactionId = newTransactionId;
                batchMaker.addRecord(record);
              } else if (lastTransactionId.equals(newTransactionId)) {
                batchMaker.addRecord(record);
              } else {
                // The Transaction ID Column Name config should not be used with MySQL as it
                // does not provide a change log table and the JDBC driver may not support scrollable cursors.
                resultSet.relative(-1);
                break; // Complete this batch without including the new record.
              }
            } else {
              batchMaker.addRecord(record);
            }
          }

          // Get the offset column value for this record
          if (isIncrementalMode) {
            nextSourceOffset = resultSet.getString(offsetColumn);
          } else {
            nextSourceOffset = initialOffset;
          }
          ++rowCount;
          ++queryRowCount;
          ++noMoreDataRecordCount;
          shouldFire = true;
        }

        if (LOG.isDebugEnabled()){
          LOG.debug("Processed rows: {}", rowCount);
        }

        if (!haveNext || rowCount == 0) {
          // We didn't have any data left in the cursor. Close everything
          // We may not have the statement here if we're not producing the
          // same batch as when we got it, so get it from the result set
          // Get it before we close the result set, just to be safe!
          statement = resultSet.getStatement();
          closeQuietly(resultSet);
          closeQuietly(statement);
          closeQuietly(connection);
          lastQueryCompletedTime = System.currentTimeMillis();
          LOG.debug("Query completed at: {}", lastQueryCompletedTime);
          JDBCQuerySuccessEvent.EVENT_CREATOR.create(getContext())
              .with(QUERY, preparedQuery)
              .with(TIMESTAMP, lastQueryCompletedTime)
              .with(ROW_COUNT, queryRowCount)
              .with(SOURCE_OFFSET, nextSourceOffset)
              .createAndSend();

          // In case of non-incremental mode, we need to generate no-more-data event as soon as we hit end of the
          // result set. Incremental mode will try to run the query again and generate the event if and only if
          // the next query results in zero rows.
          if (!isIncrementalMode) {
            generateNoMoreDataEvent();
          }
        }

        /*
         * We want to generate no-more data event on next batch if:
         * 1) We run a query in this batch and returned empty.
         * 2) We consumed at least some data since last time (to not generate the event all the time)
         */

        if (isIncrementalMode && rowCount == 0 && !haveNext && shouldFire && !firstTime) {
          generateNoMoreDataEvent();
          shouldFire = false;
        }
        firstTime = false;

      } catch (SQLException e) {
        if (++numQueryErrors == 1) {
          firstQueryException = e;
        }
        String formattedError = jdbcUtil.formatSqlException(e);
        LOG.error(formattedError, e);
        if (resultSet != null) {
          try {
            statement = resultSet.getStatement();
          } catch (SQLException e1) {
            LOG.debug("Error while getting statement from result set: {}", e1.toString(), e1);
          }
          closeQuietly(resultSet);
          closeQuietly(statement);
        }
        closeQuietly(connection);
        lastQueryCompletedTime = System.currentTimeMillis();
        JDBCQueryFailureEvent.EVENT_CREATOR.create(getContext())
            .with(QUERY, preparedQuery)
            .with(TIMESTAMP, lastQueryCompletedTime)
            .with(ERROR, formattedError)
            .with(ROW_COUNT, queryRowCount)
            .with(SOURCE_OFFSET, nextSourceOffset)
            .createAndSend();
        LOG.debug("Query '{}' failed at: {}; {} errors so far", preparedQuery, lastQueryCompletedTime, numQueryErrors);
        if (numQueryErrors > commonSourceConfigBean.numSQLErrorRetries) {
          throw new StageException(
              JdbcErrors.JDBC_77,
              e.getClass().getSimpleName(),
              preparedQuery,
              numQueryErrors,
              jdbcUtil.formatSqlException(firstQueryException)
          );
        } // else allow nextSourceOffset to be returned, to retry
      }
    }
    return nextSourceOffset;
  }

  protected ResultSet executeQuery(Statement statement, String preparedQuery) throws SQLException {
    return statement.executeQuery(preparedQuery);
  }

  protected Statement getStatement(Connection connection, int resultSetType, int resultSetConcurrency) throws SQLException {
    return connection.createStatement(resultSetType, resultSetConcurrency);
  }

  protected Connection getProduceConnection() throws SQLException {
    return dataSource.getConnection();
  }

  private void generateNoMoreDataEvent() {
    NoMoreDataEvent.EVENT_CREATOR.create(getContext())
        .with(NoMoreDataEvent.RECORD_COUNT, noMoreDataRecordCount)
        .createAndSend();
    noMoreDataRecordCount = 0;
  }

  private boolean continueReading(int rowCount, int batchSize) {
    if (txnColumnName.isEmpty()) {
      return rowCount < batchSize;
    } else {
      return rowCount < txnMaxSize;
    }
  }

  private void closeQuietly(AutoCloseable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception ex) {
        LOG.debug("Error while closing: {}", ex.toString(), ex);
      }
    }
  }

  @VisibleForTesting
  String prepareQuery(String query, String lastSourceOffset) {
    final String offset = null == lastSourceOffset ? initialOffset : lastSourceOffset;
    return query.replaceAll("\\$\\{(offset|OFFSET)}", offset);
  }

  protected Record processRow(ResultSet resultSet, long rowCount) throws SQLException {
    Source.Context context = getContext();
    ResultSetMetaData md = resultSet.getMetaData();
    int numColumns = md.getColumnCount();

    LinkedHashMap<String, Field> fields = jdbcUtil.resultSetToFields(
        resultSet,
        commonSourceConfigBean,
        errorRecordHandler,
        unknownTypeAction,
        null,
        hikariConfigBean.getVendor()
    );

    if (fields.size() != numColumns) {
      errorRecordHandler.onError(JdbcErrors.JDBC_35, fields.size(), numColumns);
      return null; // Don't output this record.
    }

    final String recordContext = StringUtils.substring(query.replaceAll("[\n\r]", ""), 0, 100) + "::rowCount:" + rowCount + (StringUtils.isEmpty(offsetColumn) ? "" : ":" + resultSet.getString(offsetColumn));
    Record record = context.createRecord(recordContext);
    if (jdbcRecordType == JdbcRecordType.LIST_MAP) {
      record.set(Field.createListMap(fields));
    } else if (jdbcRecordType == JdbcRecordType.MAP) {
      record.set(Field.create(fields));
    } else {
      // type is LIST
      List<Field> row = new ArrayList<>();
      for (Map.Entry<String, Field> fieldInfo : fields.entrySet()) {
        Map<String, Field> cell = new HashMap<>();
        cell.put("header", Field.create(fieldInfo.getKey()));
        cell.put("value", fieldInfo.getValue());
        row.add(Field.create(cell));
      }
      record.set(Field.create(row));
    }
    if (createJDBCNsHeaders) {
      jdbcUtil.setColumnSpecificHeaders(record, Collections.emptySet(), md, jdbcNsHeaderPrefix);
    }
    // We will add cdc operation type to record header even if createJDBCNsHeaders is false
    // we currently support CDC on only MS SQL.
    if (hikariConfigBean.getConnectionString().startsWith("jdbc:sqlserver")) {
      MSOperationCode.addOperationCodeToRecordHeader(record);
    }

    return record;
  }
}
