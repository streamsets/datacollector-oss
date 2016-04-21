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
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.stage.destination.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.lib.ErrorRecordHandler;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.ExternalConsistencyMode;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduException;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.PartialRow;
import org.kududb.client.RowError;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private final ConsistencyMode consistencyMode;
  private final List<KuduFieldMappingConfig> fieldMappingConfigs;
  private final int operationTimeout;
  private final LoadingCache<String, KuduTable> kuduTables = CacheBuilder.newBuilder()
      .maximumSize(500)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<String, KuduTable>() {
        @Override
        public KuduTable load(String tableName) throws Exception {
          if (kuduClient.tableExists(tableName)) {
            return kuduClient.openTable(tableName);
          } else {
            throw new KuduTableNotFoundException();
          }
        }
      });

  private ErrorRecordHandler errorRecordHandler;
  private ELVars tableNameVars;
  private ELEval tableNameEval;
  private KuduClient kuduClient;
  private KuduSession kuduSession;

  public KuduTarget(
    String kuduMaster,
    String tableNameTemplate,
    ConsistencyMode consistencyMode,
    List<KuduFieldMappingConfig> fieldMappingConfigs,
    int operationTimeout
  ) {
    this.kuduMaster = Strings.nullToEmpty(kuduMaster).trim();
    this.tableNameTemplate = Strings.nullToEmpty(tableNameTemplate).trim();
    this.consistencyMode = consistencyMode;
    this.fieldMappingConfigs = fieldMappingConfigs == null ? Collections.<KuduFieldMappingConfig>emptyList() :
      fieldMappingConfigs;
    this.operationTimeout = operationTimeout;
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
      issues.add(getContext().createConfigIssue(Groups.KUDU.name(), TABLE_NAME_TEMPLATE, Errors.KUDU_02));
    }
    if (tableNameTemplate.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.KUDU.name(), TABLE_NAME_TEMPLATE, Errors.KUDU_02));
    }

    kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).defaultOperationTimeoutMs(operationTimeout).build();
    if (issues.isEmpty()) {
      kuduSession = openKuduSession(issues);
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
                    TABLE_NAME_TEMPLATE,
                    Errors.KUDU_01,
                    tableNameTemplate
                )
            );
          } else {
            table = kuduTables.getUnchecked(tableNameTemplate);
          }
        } catch (Exception ex) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.KUDU.name(),
                  KUDU_MASTER,
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
    KuduSession session = null;
    try {
      session = kuduClient.newSession();
      try {
        session.setExternalConsistencyMode(ExternalConsistencyMode.valueOf(consistencyMode.name()));
      } catch (IllegalArgumentException ex) {
        issues.add(getContext().createConfigIssue(Groups.KUDU.name(), CONSISTENCY_MODE, Errors.KUDU_02));
      }
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    } catch (KuduException ex) {
      issues.add(getContext().createConfigIssue(Groups.KUDU.name(), KUDU_MASTER, Errors.KUDU_00, ex.toString(), ex));
    }
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
                  FIELD_MAPPING_CONFIGS,
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
        table = kuduTables.getUnchecked(tableName);
      } catch (UncheckedExecutionException ex) {
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

      try {
        while (it.hasNext()) {
          try {
            Record record = it.next();
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            recordConverter.convert(record, row);
            keyToRecordMap.put(insert.getRow().stringifyRowKey(), record);
            session.apply(insert);
          } catch (OnRecordErrorException onRecordError) {
            errorRecordHandler.onError(onRecordError);
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
          Insert insert = (Insert) error.getOperation();
          // TODO SDC-2701 - support update on duplicate key
          if ("ALREADY_PRESENT".equals(error.getStatus())) {
            // duplicate row key
            String rowKey = insert.getRow().stringifyRowKey();
            Record record = keyToRecordMap.get(rowKey);
            errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_08, rowKey));
          } else {
            throw new StageException(Errors.KUDU_03, error.toString());
          }
        }
      } catch (Exception ex) {
        LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
        throw throwStageException(ex);
      }
    }
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
