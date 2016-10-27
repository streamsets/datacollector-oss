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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.kudu;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KuduTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTarget.class);

  private static final ImmutableBiMap<Type, Field.Type> TYPE_MAP = ImmutableBiMap.<Type, Field.Type> builder()
    .put(Type.INT8, Field.Type.BYTE)
    .put(Type.INT16, Field.Type.SHORT)
    .put(Type.INT32, Field.Type.INTEGER)
    .put(Type.INT64, Field.Type.LONG)
    .put(Type.FLOAT, Field.Type.FLOAT)
    .put(Type.DOUBLE, Field.Type.DOUBLE)
    .put(Type.BINARY, Field.Type.BYTE_ARRAY)
    .put(Type.STRING, Field.Type.STRING)
    .put(Type.BOOL, Field.Type.BOOLEAN)
    .build();

  private static final String EL_PREFIX = "${";
  private static final String KUDU_MASTER = "kuduMaster";
  private static final String KUDU_SESSION = "kuduSession";
  private static final String CONSISTENCY_MODE = "consistencyMode";
  private static final String TABLE_NAME_TEMPLATE = "tableNameTemplate";
  private static final String FIELD_MAPPING_CONFIGS = "fieldMappingConfigs";

  private final String kuduMaster;
  private final String tableNameTemplate;
  private final KuduConfigBean configBean;
  private final List<KuduFieldMappingConfig> fieldMappingConfigs;
  private final int tableCacheSize = 500;

  private final LoadingCache<String, KuduTable> kuduTables;

  private ErrorRecordHandler errorRecordHandler;
  private ELVars tableNameVars;
  private ELEval tableNameEval;
  private KuduClient kuduClient;
  private KuduSession kuduSession;

  private OperationType defaultOperation;

  public KuduTarget(KuduConfigBean configBean) {
    this.configBean = configBean;
    this.kuduMaster = Strings.nullToEmpty(configBean.kuduMaster).trim();
    this.tableNameTemplate = Strings.nullToEmpty(configBean.tableNameTemplate).trim();
    this.fieldMappingConfigs = configBean.fieldMappingConfigs == null
        ? Collections.<KuduFieldMappingConfig>emptyList()
        : configBean.fieldMappingConfigs;
    this.defaultOperation = configBean.defaultOperation;

    kuduTables = CacheBuilder.newBuilder()
        .maximumSize(tableCacheSize)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, KuduTable>() {
          @Override
          public KuduTable load(String tableName) throws KuduException {
            return kuduClient.openTable(tableName);
          }
        });
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    tableNameVars = getContext().createELVars();
    tableNameEval = getContext().createELEval(TABLE_NAME_TEMPLATE);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    validateServerSideConfig(issues);
    return issues;
  }

  private void validateServerSideConfig(final List<ConfigIssue> issues) {
    LOG.info("Validating connection to Kudu cluster " + kuduMaster +
      " and whether table " + tableNameTemplate + " exists and is enabled");
    if (kuduMaster.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.KUDU.name(),
              KuduConfigBean.CONF_PREFIX + TABLE_NAME_TEMPLATE,
              Errors.KUDU_02
          )
      );
    }
    if (tableNameTemplate.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.KUDU.name(),
              KuduConfigBean.CONF_PREFIX + TABLE_NAME_TEMPLATE,
              Errors.KUDU_02
          )
      );
    }

    kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).defaultOperationTimeoutMs(configBean.operationTimeout).build();
    if (issues.isEmpty()) {
      kuduSession = openKuduSession(issues);
    }

    // Check if SDC can reach the Kudu Master
    try {
      kuduClient.getTablesList();
    } catch (KuduException ex) {
      issues.add(
          getContext().createConfigIssue(
              Groups.KUDU.name(),
              KuduConfigBean.CONF_PREFIX + KUDU_MASTER,
              Errors.KUDU_00,
              ex.toString(),
              ex
          )
      );
    }

    if (tableNameTemplate.contains(EL_PREFIX)) {
      ELUtils.validateExpression(
          tableNameEval,
          tableNameVars,
          tableNameTemplate,
          getContext(),
          Groups.KUDU.getLabel(),
          TABLE_NAME_TEMPLATE,
          Errors.KUDU_12,
          String.class,
          issues
      );
    } else {
      KuduTable table = null;
      if (issues.isEmpty()) {
        try {
          if (!kuduClient.tableExists(tableNameTemplate)) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.KUDU.name(),
                    KuduConfigBean.CONF_PREFIX + TABLE_NAME_TEMPLATE,
                    Errors.KUDU_01,
                    tableNameTemplate
                )
            );
          } else {
            // ExecutionException is thrown by LoadingCache when kuduClient.openTable() throws KuduException.
            table = kuduTables.get(tableNameTemplate);
          }
        } catch (ExecutionException | KuduException ex) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.KUDU.name(),
                  KuduConfigBean.CONF_PREFIX + KUDU_MASTER,
                  Errors.KUDU_00,
                  ex.toString(),
                  ex
              )
          );
        }
      }
      if (issues.isEmpty()) {
        createKuduRecordConverter(issues, table);
      }
    }
  }

  private KuduSession openKuduSession(List<ConfigIssue> issues) {
    KuduSession session = kuduClient.newSession();
    try {
      session.setExternalConsistencyMode(ExternalConsistencyMode.valueOf(configBean.consistencyMode.name()));
    } catch (IllegalArgumentException ex) {
      issues.add(
          getContext().createConfigIssue(
              Groups.KUDU.name(),
              KuduConfigBean.CONF_PREFIX + CONSISTENCY_MODE,
              Errors.KUDU_02
          )
      );
    }
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    return session;
  }

  private Optional<KuduRecordConverter> createKuduRecordConverter(KuduTable table) {
    return createKuduRecordConverter(null, table);
  }

  private Optional<KuduRecordConverter> createKuduRecordConverter(List<ConfigIssue> issues, KuduTable table) {
    Map<String, Field.Type> columnsToFieldTypes = new HashMap<>();
    Map<String, String> fieldsToColumns = new HashMap<>();
    Schema schema = table.getSchema();
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Field.Type type = TYPE_MAP.get(columnSchema.getType());
      if (type == null) {
        // don't set a default mapping for columns we don't understand
        LOG.warn(Errors.KUDU_10.getMessage(), columnSchema.getName(), columnSchema.getType());
      } else {
        columnsToFieldTypes.put(columnSchema.getName(), type);
        fieldsToColumns.put("/" + columnSchema.getName(), columnSchema.getName());
      }
    }
    for (KuduFieldMappingConfig fieldMappingConfig : fieldMappingConfigs) {
      Field.Type type = columnsToFieldTypes.get(fieldMappingConfig.columnName);
      if (type == null) {
        LOG.error(Errors.KUDU_05.getMessage(), fieldMappingConfig.columnName);
        if (issues != null) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.KUDU.name(),
                  KuduConfigBean.CONF_PREFIX + FIELD_MAPPING_CONFIGS,
                  Errors.KUDU_05,
                  fieldMappingConfig.columnName
              )
          );
        }
      } else {
        fieldsToColumns.remove("/" + fieldMappingConfig.columnName); // remove default mapping
        fieldsToColumns.put(fieldMappingConfig.field, fieldMappingConfig.columnName);
      }
    }
    return Optional.of(new KuduRecordConverter(columnsToFieldTypes, fieldsToColumns, schema));
  }

  @Override
  public void write(final Batch batch) throws StageException {
    try {
      writeBatch(batch);
    } catch (Exception e) {
      throw throwStageException(e);
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.KUDU_03, cause, cause);
      }
    } else if (e instanceof StageException) {
      return (StageException)e;
    }
    return new StageException(Errors.KUDU_03, e, e);
  }

  private void writeBatch(Batch batch) throws StageException {
    Multimap<String, Record> partitions = ELUtils.partitionBatchByExpression(
        tableNameEval,
        tableNameVars,
        tableNameTemplate,
        batch
    );

    KuduSession session = Preconditions.checkNotNull(kuduSession, KUDU_SESSION);

    for (String tableName : partitions.keySet()) {
      Map<String, Record> keyToRecordMap = new HashMap<>();
      Iterator<Record> it = partitions.get(tableName).iterator();

      // if table doesn't exist, send records to the error handler and continue
      KuduTable table;
      try {
        table = kuduTables.get(tableName);
      } catch (ExecutionException ex) {
        while (it.hasNext()) {
          errorRecordHandler.onError(new OnRecordErrorException(it.next(), Errors.KUDU_01, tableName));
        }
        continue;
      }

      Optional<KuduRecordConverter> kuduRecordConverter = createKuduRecordConverter(table);
      if (!kuduRecordConverter.isPresent()) {
        throw new StageException(Errors.KUDU_11);
      }
      KuduRecordConverter recordConverter = kuduRecordConverter.get();

      Record record = null;
      try {
        while (it.hasNext()) {
          try {
            record = it.next();
            Operation operation;
            Optional<String> optOperation = Optional.fromNullable(
                record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE)
            );
            String op = optOperation.or(defaultOperation.getLabel()).toUpperCase();
            operation = getOperation(table, op);
            PartialRow row = operation.getRow();
            recordConverter.convert(record, row, op);
            keyToRecordMap.put(operation.getRow().stringifyRowKey(), record);
            session.apply(operation);
          } catch (StageException err) {
            errorRecordHandler.onError(new OnRecordErrorException(record, err.getErrorCode(), err.getMessage()));
          }
        }
        List<RowError> rowErrors = Collections.emptyList();
        List<OperationResponse> responses = session.flush(); // can return null
        if (responses != null) {
          rowErrors = OperationResponse.collectErrors(responses);
        }
        // log ALL errors then process them
        for (RowError error : rowErrors) {
          LOG.warn(Errors.KUDU_03.getMessage(), error.toString());
        }
        for (RowError error : rowErrors) {
          if (error.getErrorStatus().isAlreadyPresent()) {
            // duplicate row key
            Operation operation = error.getOperation();
            String rowKey = operation.getRow().stringifyRowKey();
            Record errorRecord = keyToRecordMap.get(rowKey);
            errorRecordHandler.onError(new OnRecordErrorException(errorRecord, Errors.KUDU_08, rowKey));
          } else {
            throw new StageException(Errors.KUDU_03, error.toString());
          }
        }
      } catch (KuduException ex) {
        LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_03, ex.getMessage(), ex));
      } catch (StageException ex) {
        LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
        errorRecordHandler.onError(ex.getErrorCode(), ex.getMessage(), ex);
      }
    }
  }

  private Operation getOperation(KuduTable table, String op) throws StageException {
    Operation operation = null;
    try {
      switch (OperationType.getTypeFromString(op)) {
        case INSERT:
          operation = table.newInsert();
          break;
        case UPSERT:
          operation = table.newUpsert();
          break;
        case UPDATE:
        case SELECT_FOR_UPDATE:
        case AFTER_UPDATE:
          operation = table.newUpdate();
          break;
        case DELETE:
          operation = table.newDelete();
          break;
        default:
          LOG.error("Operation {} not supported", op);
          throw new StageException(Errors.KUDU_13, op);
      }
    } catch (UnsupportedOperationException ex){
      // get here if op is unsupported operation
      throw new StageException(Errors.KUDU_13, op);
    }
    return operation;
  }

  @Override
  public void destroy() {
    if (kuduClient != null) {
      try {
        kuduClient.close();
      } catch (Exception ex) {
        LOG.warn("Error closing Kudu connection: {}", ex.toString(), ex);
      }
    }
    kuduClient = null;
    kuduSession = null;
    kuduTables.invalidateAll();
    super.destroy();
  }
}
