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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcMetastoreUtil;
import com.streamsets.pipeline.lib.jdbc.JdbcSchemaReader;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriterFactory;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JdbcMetadataProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataProcessor.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String JDBC_SCHEMA = "schemaEL";
  private static final String JDBC_TABLE_NAME = "tableNameEL";

  private final String schemaEL;
  private final String tableNameEL;
  private final HikariPoolConfigBean hikariConfigBean;
  private final DecimalDefaultsConfig decimalDefaultsConfig;

  // Database cache only holds metadata for databases
  private LoadingCache<Pair<String, String>, LinkedHashMap<String, JdbcTypeInfo>> tableCache;

  private JdbcMetadataProcessorELEvals elEvals = new JdbcMetadataProcessorELEvals();

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private JdbcSchemaReader schemaReader;
  private JdbcSchemaWriter schemaWriter;

  private static class JdbcMetadataProcessorELEvals {
    private ELEval dbNameELEval;
    private ELEval tableNameELEval;

    public void init(Stage.Context context) {
      dbNameELEval = context.createELEval(JDBC_SCHEMA);
      tableNameELEval = context.createELEval(JDBC_TABLE_NAME);
    }
  }

  JdbcMetadataProcessor(JdbcMetadataConfigBean configBean) {
    this.schemaEL = configBean.schemaEL;
    this.tableNameEL = configBean.tableNameEL;
    this.hikariConfigBean = configBean.hikariConfigBean;
    this.decimalDefaultsConfig = configBean.decimalDefaultsConfig;
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    elEvals.init(getContext());

    Processor.Context context = getContext();

    issues.addAll(hikariConfigBean.validateConfigs(context, issues));

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = JdbcUtil.createDataSourceForWrite(hikariConfigBean, null, null,
            false,
            issues,
            Collections.emptyList(),
            getContext()
        );
      } catch (RuntimeException | SQLException | StageException e) {
        LOG.debug("Could not connect to data source", e);
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    if (issues.isEmpty()) {
      try {
        schemaWriter = JdbcSchemaWriterFactory.create(hikariConfigBean.connectionString, dataSource);
      } catch (JdbcStageCheckedException e) {
        issues.add(getContext().createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, e.getErrorCode(), e.getParams()));
      }
      schemaReader = new JdbcSchemaReader(dataSource, schemaWriter);

      tableCache = CacheBuilder.newBuilder().maximumSize(50).build(new CacheLoader<Pair<String, String>, LinkedHashMap<String, JdbcTypeInfo>>() {
        @Override
        public LinkedHashMap<String, JdbcTypeInfo> load(Pair<String, String> pair) throws Exception {
          return schemaReader.getTableSchema(pair.getLeft(), pair.getRight());
        }
      });
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    JdbcUtil.closeQuietly(dataSource);
    super.destroy();
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    try {
      ELVars variables = getContext().createELVars();
      RecordEL.setRecordInContext(variables, record);
      TimeEL.setCalendarInContext(variables, Calendar.getInstance());
      TimeNowEL.setTimeNowInContext(variables, new Date());

      String schema = (schemaEL != null) ? elEvals.dbNameELEval.eval(variables, schemaEL, String.class) : null;
      String tableName = elEvals.tableNameELEval.eval(variables, tableNameEL, String.class);

      if (StringUtils.isEmpty(schema)) {
        schema = null;
      }

      // Obtain the record structure from current record
      LinkedHashMap<String, JdbcTypeInfo> recordStructure = JdbcMetastoreUtil.convertRecordToJdbcType(
          record,
          decimalDefaultsConfig.precisionAttribute,
          decimalDefaultsConfig.scaleAttribute,
          schemaWriter);

      if (recordStructure.isEmpty()) {
        batchMaker.addRecord(record);
        return;
      }

      LinkedHashMap<String, JdbcTypeInfo> tableStructure = null;
      try {
        tableStructure = tableCache.get(Pair.of(schema, tableName));
      } catch (ExecutionException e) {
        throw new JdbcStageCheckedException(JdbcErrors.JDBC_203, e.getMessage(), e);
      }

      if (tableStructure.isEmpty()) {
        // Create table
        schemaWriter.createTable(schema, tableName, recordStructure);
        tableCache.put(Pair.of(schema, tableName), recordStructure);
      } else {
        // Compare tables
        LinkedHashMap<String, JdbcTypeInfo> columnDiff = JdbcMetastoreUtil.getDiff(tableStructure, recordStructure);
        if (!columnDiff.isEmpty()) {
          LOG.trace("Detected drift for table {} - new columns: {}",
              tableName,
              StringUtils.join(columnDiff.keySet(), ",")
          );
          schemaWriter.alterTable(schema, tableName, columnDiff);
          tableCache.put(Pair.of(schema, tableName), recordStructure);
        }
      }

      batchMaker.addRecord(record);
    } catch (JdbcStageCheckedException error) {
      LOG.error("Error happened when processing record", error);
      LOG.trace("Record that caused the error: {}", record.toString());
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }
  }
}
