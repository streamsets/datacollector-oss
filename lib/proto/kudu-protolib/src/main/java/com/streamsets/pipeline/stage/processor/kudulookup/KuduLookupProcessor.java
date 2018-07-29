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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.lib.kudu.Errors;
import com.streamsets.pipeline.stage.lib.kudu.KuduFieldMappingConfig;
import com.streamsets.pipeline.stage.lib.kudu.KuduUtils;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class KuduLookupProcessor extends SingleLaneRecordProcessor {
  private static final String KUDU_MASTER = "kuduMaster";
  private static final String KUDU_TABLE = "kuduTableTemplate";
  private static final String KEY_MAPPING_CONFIGS = "keyColumnMapping";
  private static final String OUTPUT_MAPPING_CONFIG = "outputColumnMapping";
  private static final String EL_PREFIX = "${";
  private static final String OPERATION_TIMEOUT = "operationTimeout";
  private static final String ADMIN_OPERATION_TIMEOUT = "adminOperationTimeout";

  private static final Logger LOG = LoggerFactory.getLogger(KuduLookupProcessor.class);
  private KuduLookupConfig conf;
  private ErrorRecordHandler errorRecordHandler;
  private AsyncKuduClient kuduClient;
  private AsyncKuduSession kuduSession;
  private KuduTable kuduTable;
  private KuduLookupLoader store;

  private final List<String> keyColumns = new ArrayList<>();
  private final Map<String, String> columnToField = new HashMap<>();
  private ELEval tableNameEval;
  private ELVars tableNameVars;

  private LoadingCache<KuduLookupKey, List<Map<String, Field>>> cache;
  private CacheCleaner cacheCleaner;

  public KuduLookupProcessor(KuduLookupConfig conf) {
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    tableNameVars = getContext().createELVars();
    tableNameEval = getContext().createELEval(KUDU_TABLE);

    if (conf.keyColumnMapping.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.KUDU.name(),
          KuduLookupConfig.CONF_PREFIX + KEY_MAPPING_CONFIGS,
          Errors.KUDU_30
      ));
    }

    if (conf.outputColumnMapping.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.KUDU.name(),
          KuduLookupConfig.CONF_PREFIX + OUTPUT_MAPPING_CONFIG,
          Errors.KUDU_30
      ));
    }

    for (KuduFieldMappingConfig fieldConfig : conf.keyColumnMapping) {
      String columnName = conf.caseSensitive ? fieldConfig.columnName : fieldConfig.columnName.toLowerCase();
      keyColumns.add(columnName);
      columnToField.put(columnName, fieldConfig.field);
    }

    if (conf.operationTimeout < 0) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ADVANCED.name(),
              KuduLookupConfig.CONF_PREFIX + OPERATION_TIMEOUT,
              Errors.KUDU_02
          )
      );
    }

    if (conf.adminOperationTimeout < 0) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ADVANCED.name(),
              KuduLookupConfig.CONF_PREFIX + ADMIN_OPERATION_TIMEOUT,
              Errors.KUDU_02
          )
      );
    }

    AsyncKuduClient.AsyncKuduClientBuilder builder = new AsyncKuduClient.AsyncKuduClientBuilder(conf.kuduMaster)
      .defaultOperationTimeoutMs(conf.operationTimeout)
      .defaultAdminOperationTimeoutMs(conf.adminOperationTimeout);

    // Caution: if number of worker thread is not configured, Kudu client may start a massive amount of worker threads.
    // The formula is "2 x available cores"
    if (conf.numWorkers > 0) {
      builder.workerCount(conf.numWorkers);
    }
    kuduClient = builder.build();

    if (issues.isEmpty()) {
      kuduSession = kuduClient.newSession();
    }

    if (issues.isEmpty()) {
      // Check if SDC can reach the Kudu Master
      KuduUtils.checkConnection(kuduClient, getContext(), KUDU_MASTER, issues);
    }
    if (issues.isEmpty()) {
      if (conf.kuduTableTemplate.contains(EL_PREFIX)) {
        ELUtils.validateExpression(
            tableNameEval,
            tableNameVars,
            conf.kuduTableTemplate,
            getContext(),
            com.streamsets.pipeline.stage.destination.kudu.Groups.KUDU.getLabel(),
            KUDU_TABLE,
            Errors.KUDU_12,
            String.class,
            issues
        );
      } else {
        // We have a table name that's not EL. We can validate if the table exists.
        String tableName = conf.caseSensitive ? conf.kuduTableTemplate : conf.kuduTableTemplate.toLowerCase();
        try {
          if (!kuduClient.tableExists(tableName).join()) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.KUDU.name(),
                    KuduLookupConfig.CONF_PREFIX + KUDU_TABLE,
                    Errors.KUDU_01,
                    tableName
                )
            );
          }
        } catch (Exception ex) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.KUDU.name(),
                  KuduLookupConfig.CONF_PREFIX + KUDU_TABLE,
                  Errors.KUDU_03,
                  ex
              )
          );
        }
      }
    }

    if (issues.isEmpty()) {
      store = new KuduLookupLoader(getContext(), kuduClient, keyColumns, columnToField, conf);
      cache = LookupUtils.buildCache(store, conf.cache);
      cacheCleaner = new CacheCleaner(cache, "KuduLookupProcessor", 10 * 60 * 1000);
    }
    return issues;
  }


  @Override
  public void destroy() {
    super.destroy();
    if (kuduSession != null) {
      try {
        List<OperationResponse> result = kuduSession.close().join();
        if (result != null && !result.isEmpty()) {
          String msg = "Unexpected operation responses from session close: " + result;
          throw new RuntimeException(msg);
        }
      } catch (Exception  e) {
        String msg = "Unexpected exception closing KuduSession: " + e;
        LOG.error(msg, e);
        throw new RuntimeException(e);
      }
    }
    if (kuduClient != null) {
      try {
        kuduClient.close();
      } catch (Exception  e) {
        String msg = "Unexpected exception closing KuduClient: " + e;
        LOG.error(msg, e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void process(Batch batch, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    super.process(batch, batchMaker);
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) throws StageException {
    RecordEL.setRecordInContext(tableNameVars, record);
    String tableName = tableNameEval.eval(tableNameVars, conf.kuduTableTemplate, String.class);
    if (!conf.caseSensitive) {
      tableName = tableName.toLowerCase();
    }
    LOG.trace("Processing record:{}  TableName={}", record.toString(), tableName);

    try {
      try {
        KuduLookupKey key = generateLookupKey(record, tableName);
        List<Map<String, Field>> values = cache.get(key);
        if (values.isEmpty()) {
          // No record found
          if (conf.missingLookupBehavior == MissingValuesBehavior.SEND_TO_ERROR) {
            errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_31));
          } else {
            // Configured to 'Send to next stage' and 'pass as it is'
            batchMaker.addRecord(record);
          }
        } else {
          switch (conf.multipleValuesBehavior) {
            case FIRST_ONLY:
              setFieldsInRecord(record, values.get(0));
              batchMaker.addRecord(record);
              break;
            case SPLIT_INTO_MULTIPLE_RECORDS:
              for(Map<String, Field> lookupItem : values) {
                Record newRecord = getContext().cloneRecord(record);
                setFieldsInRecord(newRecord, lookupItem);
                batchMaker.addRecord(newRecord);
              }
              break;
            default:
              throw new IllegalStateException("Unknown multiple value behavior: " + conf.multipleValuesBehavior);
          }
        }
      } catch (ExecutionException e) {
        Throwables.propagateIfPossible(e.getCause(), StageException.class);
        Throwables.propagateIfPossible(e.getCause(), OnRecordErrorException.class);
        throw new IllegalStateException(e); // The cache loader shouldn't throw anything that isn't a StageException.
      }
    } catch (OnRecordErrorException error) { // NOSONAR
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }
  }

  private void setFieldsInRecord(Record record, Map<String, Field> fields) {
    for (Map.Entry<String, Field> entry : fields.entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = columnToField.get(columnName);
      Field field = entry.getValue();
      if (fieldPath == null) {
        Field root = record.get();
        // No mapping
        switch (root.getType()) {
          case LIST:
            // Add new field to the end of the list
            fieldPath = "[" + root.getValueAsList().size() + "]";
            Map<String, Field> cell = new HashMap<>();
            cell.put("header", Field.create(columnName));
            cell.put("value", field);
            field = Field.create(cell);
            break;
          case LIST_MAP:
          case MAP:
            // Just use the column name
            fieldPath = columnName;
            break;
          default:
            break;
        }
      }
      record.set(fieldPath, field);
    }
  }

  /**
   * Create a map of keyColumn - value to lookup in cache.
   * @param record
   * @return Map of keyColumn - value
   * @throws OnRecordErrorException
   */
  private KuduLookupKey generateLookupKey(final Record record, final String tableName) throws OnRecordErrorException{
    Map<String, Field> keyList = new HashMap<>();
    for (Map.Entry<String, String> key : columnToField.entrySet()){
      String fieldName = key.getValue();
      if (!record.has(fieldName)) {
        throw new OnRecordErrorException(record, Errors.KUDU_32, fieldName);
      }
      keyList.put(key.getKey(), record.get(fieldName));
    }
    return new KuduLookupKey(tableName, keyList);
  }
}
