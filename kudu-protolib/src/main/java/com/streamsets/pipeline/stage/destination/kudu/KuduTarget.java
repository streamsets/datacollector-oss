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
package com.streamsets.pipeline.stage.destination.kudu;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.FieldPathConverter;
import com.streamsets.pipeline.lib.operation.MongoDBOpLogFieldConverter;
import com.streamsets.pipeline.lib.operation.MySQLBinLogFieldConverter;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.kudu.Errors;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import org.apache.commons.lang.StringUtils;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KuduTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTarget.class);

  private static final Map<Type, Field.Type> TYPE_MAP = ImmutableMap.<Type, Field.Type> builder()
    .put(Type.INT8, Field.Type.BYTE)
    .put(Type.INT16, Field.Type.SHORT)
    .put(Type.INT32, Field.Type.INTEGER)
    .put(Type.INT64, Field.Type.LONG)
    .put(Type.FLOAT, Field.Type.FLOAT)
    .put(Type.DOUBLE, Field.Type.DOUBLE)
    .put(Type.BINARY, Field.Type.BYTE_ARRAY)
    .put(Type.STRING, Field.Type.STRING)
    .put(Type.BOOL, Field.Type.BOOLEAN)
    .put(Type.UNIXTIME_MICROS, Field.Type.LONG)
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
  private final LoadingCache<KuduTable, KuduRecordConverter> recordConverters;
  private final CacheCleaner cacheCleaner;
  private final CacheCleaner recordBuilderCacheCleaner;

  private ErrorRecordHandler errorRecordHandler;
  private ELVars tableNameVars;
  private ELEval tableNameEval;
  private KuduClient kuduClient;
  private KuduSession kuduSession;

  private KuduOperationType defaultOperation;
  private Set<String> accessedTables;

  public KuduTarget(KuduConfigBean configBean) {
    this.configBean = configBean;
    this.kuduMaster = Strings.nullToEmpty(configBean.kuduMaster).trim();
    this.tableNameTemplate = Strings.nullToEmpty(configBean.tableNameTemplate).trim();
    this.fieldMappingConfigs = configBean.fieldMappingConfigs == null
        ? Collections.<KuduFieldMappingConfig>emptyList()
        : configBean.fieldMappingConfigs;
    this.defaultOperation = configBean.defaultOperation;

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
        .maximumSize(tableCacheSize)
        .expireAfterAccess(1, TimeUnit.HOURS);

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    kuduTables = cacheBuilder.build(new CacheLoader<String, KuduTable>() {
          @Override
          public KuduTable load(String tableName) throws KuduException {
            return kuduClient.openTable(tableName);
          }
        });

    cacheCleaner = new CacheCleaner(kuduTables, "KuduTarget", 10 * 60 * 1000);

    recordConverters = cacheBuilder.build(new CacheLoader<KuduTable, KuduRecordConverter>() {
      @Override
      public KuduRecordConverter load(KuduTable table) throws KuduException {
        return createKuduRecordConverter(table);
      }
    });
    recordBuilderCacheCleaner = new CacheCleaner(recordConverters, "KuduTarget KuduRecordConverter", 10 * 60 * 1000);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    tableNameVars = getContext().createELVars();
    tableNameEval = getContext().createELEval(TABLE_NAME_TEMPLATE);
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    validateServerSideConfig(issues);
    accessedTables = new HashSet<>();

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
    session.setMutationBufferSpace(configBean.mutationBufferSpace);
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    return session;
  }

  private KuduRecordConverter createKuduRecordConverter(KuduTable table) {
    return createKuduRecordConverter(null, table);
  }

  private KuduRecordConverter createKuduRecordConverter(List<ConfigIssue> issues, KuduTable table) {
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
    FieldPathConverter converter = null;
    if (configBean.changeLogFormat == ChangeLogFormat.MongoDBOpLog) {
      converter = new MongoDBOpLogFieldConverter();
    } else if (configBean.changeLogFormat == ChangeLogFormat.MySQLBinLog){
      converter = new MySQLBinLogFieldConverter();
    }
    return new KuduRecordConverter(columnsToFieldTypes, fieldsToColumns, schema, converter);
  }

  @Override
  public void write(final Batch batch) throws StageException {
    try {
      if (!batch.getRecords().hasNext()) {
        // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
        cacheCleaner.periodicCleanUp();
        recordBuilderCacheCleaner.periodicCleanUp();
      }

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

      // Send one LineageEvent per table that is accessed.
      if(!StringUtils.isEmpty(tableName)) {
        if (!accessedTables.contains(tableName)) {
          accessedTables.add(tableName);
          sendLineageEvent(tableName);
        }
      }

      Map<String, Record> keyToRecordMap = new HashMap<>();
      Iterator<Record> it = partitions.get(tableName).iterator();

      KuduTable table;
      KuduRecordConverter recordConverter;
      try {
        table = kuduTables.get(tableName);
        recordConverter = recordConverters.get(table);
      } catch (ExecutionException ex) {
        // if table doesn't exist, send records to the error handler and continue
        while (it.hasNext()) {
          errorRecordHandler.onError(new OnRecordErrorException(it.next(), Errors.KUDU_01, tableName));
        }
        continue;
      }

      while (it.hasNext()) {
        Record record = null;
        try {
          record = it.next();
          Operation operation = null;
          int opCode = -1;
          String op = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
          // Check if the operation code from header attribute is valid
          if (op != null && !op.isEmpty()) {
            try {
              opCode = KuduOperationType.convertToIntCode(op);
              operation = getOperation(table, opCode);
            } catch (NumberFormatException | UnsupportedOperationException ex) {
              // Operation obtained from header is not supported. Handle accordingly
              switch (configBean.unsupportedAction) {
                case DISCARD:
                  LOG.debug("Discarding record with unsupported operation {}", op);
                  break;
                case SEND_TO_ERROR:
                  errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_13, ex.getMessage()));
                  break;
                case USE_DEFAULT:
                  opCode = defaultOperation.code;
                  operation = getOperation(table, opCode);
                  break;
                default: //unknown action
                  errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_14, ex.getMessage(), ex));
              }
            }
          } else {
            // No header attribute set. Use default.
            opCode = defaultOperation.code;
            operation = getOperation(table, opCode);
          }
          if (operation != null) {
            PartialRow row = operation.getRow();
            recordConverter.convert(record, row, opCode);
            LOG.trace("Parameters in query: OpCode:{}, {}",
                opCode,
                operation.getRow().toString()
            );
            try {
              keyToRecordMap.put(operation.getRow().stringifyRowKey(), record);
              session.apply(operation);
            } catch (IllegalStateException ex) {
              // IllegalStateException is thrown when there is issue in column values
              LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
              errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_03, ex.getMessage(), ex));
            }
          }
        } catch (OnRecordErrorException e) {
          errorRecordHandler.onError(e);
        } catch (KuduException ex) {
          LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_03, ex.getMessage(), ex));
        }
      }
      // from here, executed at the end of batch
      try {
        List<RowError> rowErrors = Collections.emptyList();
        List<OperationResponse> responses = session.flush();
        if (responses != null) {
          rowErrors = OperationResponse.collectErrors(responses);
        }
        // log ALL errors then process them
        for (RowError error : rowErrors) {
          LOG.warn(Errors.KUDU_03.getMessage(), error.toString());
        }
        for (RowError error : rowErrors) {
          Operation operation = error.getOperation();
          String rowKey = operation.getRow().stringifyRowKey();
          Record errorRecord = keyToRecordMap.get(rowKey);
          if (error.getErrorStatus().isAlreadyPresent()) {
            // Failed due to inserting duplicate row key
            errorRecordHandler.onError(new OnRecordErrorException(errorRecord, Errors.KUDU_08, rowKey));
          } else if (error.getErrorStatus().isNotFound()) {
            // Row key not found error, mostly for update and delete operations.
            errorRecordHandler.onError(new OnRecordErrorException(errorRecord, Errors.KUDU_15, rowKey));
          } else {
            // Failure is most likely caused by setting, network, or corrupted table.
            // Worth throwing StageException.
            throw new StageException(Errors.KUDU_03, error.toString());
          }
        }
      } catch (KuduException ex) {
        LOG.error(Errors.KUDU_03.getMessage(), ex.toString(), ex);
        throw new StageException(Errors.KUDU_03, ex.getMessage(), ex);
      }
    }
  }

  /**
   * Return Operation based on the operation code. If the code has a number
   * that Kudu destination doesn't support, it throws UnsupportedOperationException.
   * @param table
   * @param op
   * @return
   * @throws UnsupportedOperationException
   */
  protected Operation getOperation(KuduTable table, int op) throws UnsupportedOperationException {
    Operation operation = null;
    switch (op) {
      case OperationType.INSERT_CODE:
        operation = table.newInsert();
        break;
      case OperationType.UPSERT_CODE:
        operation = table.newUpsert();
        break;
      case OperationType.UPDATE_CODE:
        operation = table.newUpdate();
        break;
      case OperationType.DELETE_CODE:
        operation = table.newDelete();
        break;
      default:
        LOG.error("Operation {} not supported", op);
        throw new UnsupportedOperationException(String.format("Unsupported Operation: %s", op));
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

  private void sendLineageEvent(String tableName) {

    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_WRITTEN);
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.KUDU.name());
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, tableName);
    event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, configBean.kuduMaster);
    getContext().publishLineageEvent(event);

  }
}
