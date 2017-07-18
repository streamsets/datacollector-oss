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
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.kudu.Errors;
import com.streamsets.pipeline.stage.destination.kudu.Groups;
import com.streamsets.pipeline.stage.destination.kudu.KuduFieldMappingConfig;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KuduLookupProcessor extends SingleLaneRecordProcessor {
  private static final String KUDU_MASTER = "kuduMaster";
  private static final String KUDU_TABLE = "kuduTable";
  private static final String FIELD_MAPPING_CONFIGS = "fieldMappingConfigs";

  private static final Logger LOG = LoggerFactory.getLogger(KuduLookupProcessor.class);
  private KuduLookupConfig conf;
  private ErrorRecordHandler errorRecordHandler;
  private KuduClient kuduClient;
  private KuduSession kuduSession;
  private KuduTable kuduTable;
  private KuduLookupLoader store;
  private final List<String> projectColumns = new ArrayList<>();
  private final Map<String, String> columnToField = new HashMap<>();
  private final Map<String, String> fieldToColumn = new HashMap<>();
  private LoadingCache<Record, List<Map<String, Field>>> cache;
  private CacheCleaner cacheCleaner;

  public KuduLookupProcessor(KuduLookupConfig conf) {
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (conf.fieldMappingConfigs.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.KUDU.name(),
          KuduLookupConfig.CONF_PREFIX + FIELD_MAPPING_CONFIGS,
          Errors.KUDU_30
      ));
    }

    for (KuduFieldMappingConfig fieldConfig : conf.fieldMappingConfigs) {
      projectColumns.add(fieldConfig.columnName);
      columnToField.put(fieldConfig.columnName, fieldConfig.field);
      fieldToColumn.put(fieldConfig.field, fieldConfig.columnName);
    }

    kuduClient = new KuduClient.KuduClientBuilder(conf.kuduMaster).defaultOperationTimeoutMs(conf.operationTimeout)
        .build();
    if (issues.isEmpty()) {
      kuduSession = kuduClient.newSession();
    }

    if (issues.isEmpty()) {
      // Check if SDC can reach the Kudu Master
      try {
        kuduClient.getTablesList();
      } catch (KuduException ex) {
        issues.add(
            getContext().createConfigIssue(
                Groups.KUDU.name(),
                KuduLookupConfig.CONF_PREFIX + KUDU_MASTER,
                Errors.KUDU_00,
                ex.toString(),
                ex
            )
        );
      }
    }

    if (issues.isEmpty()) {
      try {
        kuduTable = kuduClient.openTable(conf.kuduTable);
      } catch (KuduException ex) {
        issues.add(
            getContext().createConfigIssue(
                Groups.KUDU.name(),
                KuduLookupConfig.CONF_PREFIX + KUDU_TABLE,
                Errors.KUDU_01,
                conf.kuduTable,
                ex
            )
        );
      }
    }

    if (issues.isEmpty()) {
      store = new KuduLookupLoader(getContext(), kuduClient, kuduTable, projectColumns, columnToField, fieldToColumn,
          conf);
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
        List<OperationResponse> result = kuduSession.close();
        if (result != null && !result.isEmpty()) {
          String msg = "Unexpected operation responses from session close: " + result;
          throw new RuntimeException(msg);
        }
      } catch (IOException  e) {
        String msg = "Unexpected exception closing KuduSession: " + e;
        LOG.error(msg, e);
        throw new RuntimeException(e);
      }
    }
    if (kuduClient != null) {
      try {
        kuduClient.close();
      } catch (IOException  e) {
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
    try {
      try {
        List<Map<String, Field>> values = cache.get(record);
        if (values.isEmpty()) {
          // No results
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.KUDU_31));
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

  private void setFieldsInRecord(Record record, Map<String, Field>fields) {
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
            fieldPath = "/" + columnName;
            break;
          default:
            break;
        }
      }
      record.set(fieldPath, field);
    }
  }
}