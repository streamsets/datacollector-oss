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
package com.streamsets.pipeline.stage.bigquery.destination;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BigQueryTarget extends BaseTarget {
  private static final Joiner COMMA_JOINER = Joiner.on(",");
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTarget.class);

  private final BigQueryTargetConfig conf;

  private BigQuery bigQuery;
  private ELEval dataSetEval;
  private ELEval tableNameELEval;

  BigQueryTarget(BigQueryTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    BigQueryDelegate.getCredentials(getContext(), issues, conf.credentials).ifPresent(c ->
        bigQuery = BigQueryDelegate.getBigquery(c, conf.credentials.projectId)
    );
    dataSetEval = getContext().createELEval("datasetEL");
    tableNameELEval = getContext().createELEval("tableNameEL");
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Map<TableId, List<Record>> tableIdToRecords = new LinkedHashMap<>();
    Map<Long, Record> indexToRecords = new LinkedHashMap<>();

    final AtomicLong index = new AtomicLong(0);

    ELVars elVars = getContext().createELVars();

    batch.getRecords().forEachRemaining(record -> {
      indexToRecords.put(index.incrementAndGet(), record);
      RecordEL.setRecordInContext(elVars, record);
      try {
        String datasetName = dataSetEval.eval(elVars, conf.datasetEL, String.class);
        String tableName = tableNameELEval.eval(elVars, conf.tableNameEL, String.class);
        TableId tableId = TableId.of(datasetName, tableName);
        List<Record> tableIdRecords = tableIdToRecords.computeIfAbsent(tableId, t -> new ArrayList<>());
        tableIdRecords.add(record);
      } catch (ELEvalException e) {
        LOG.error("Error evaluating EL.", e);
        getContext().toError(record, Errors.BIGQUERY_10, e);
      }
    });

    tableIdToRecords.forEach((tableId, records) -> {
      InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
      records.forEach(record -> {
            try {
              Map<String, ?> rowContent = conf.implicitFieldMapping ?
                  covertToRowObjectFromRecordImplicitly(record) :
                  covertToRowObjectFromRecordExplicitly(
                      record,
                      conf.bigQueryFieldMappingConfigs,
                      conf.ignoreInvalidColumn
                  );
              if (rowContent.isEmpty()) {
                throw new OnRecordErrorException(record, Errors.BIGQUERY_14);
              }
              insertAllRequestBuilder.addRow(record.getHeader().getSourceId(), rowContent);
            } catch (OnRecordErrorException e) {
              LOG.error("Error when converting record {} to row, Reason : {} ", record.getHeader().getSourceId(), e.getMessage());
              getContext().toError(record, e.getErrorCode(), e.getParams());
            }
          }
      );

      insertAllRequestBuilder.setIgnoreUnknownValues(conf.ignoreInvalidColumn);
      insertAllRequestBuilder.setSkipInvalidRows(false);

      InsertAllRequest request = insertAllRequestBuilder.build();

      if (!request.getRows().isEmpty()) {
        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {
          response.getInsertErrors().forEach((recordId, errors) -> {
            Record record = indexToRecords.get(recordId);
            String messages = COMMA_JOINER.join(errors.stream().map(BigQueryError::getMessage).collect(Collectors.toList()));
            String reasons = COMMA_JOINER.join(errors.stream().map(BigQueryError::getReason).collect(Collectors.toList()));
            LOG.error("Error when inserting record {}, Reasons : {}, Messages : {}", record.getHeader().getSourceId(), reasons, messages);
            getContext().toError(record, Errors.BIGQUERY_11, reasons, messages);
          });
        }
      }
    });
  }

  /**
   * Convert the root field to a java map object implicitly mapping each field to the column (only non nested objects)
   * @param record record to be converted
   * @return Java row representation for the record
   */
  private Map<String, Object> covertToRowObjectFromRecordImplicitly(Record record) throws OnRecordErrorException {
    Field rootField = record.get();
    Map<String, Object> rowObject = new LinkedHashMap<>();
    if (rootField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      Map<String, Field> fieldMap = rootField.getValueAsMap();
      for (Map.Entry<String, Field> fieldEntry : fieldMap.entrySet()) {
        Field field = fieldEntry.getValue();
        if (field.getType().isOneOf(Field.Type.MAP, Field.Type.LIST, Field.Type.LIST_MAP, Field.Type.FILE_REF)) {
          throw new OnRecordErrorException(record,  Errors.BIGQUERY_12, "/" + fieldEntry.getKey());
        }
        rowObject.put(fieldEntry.getKey(), field.getValue());
      }
    } else {
      throw new OnRecordErrorException(record,  Errors.BIGQUERY_12, "/");
    }
    return rowObject;
  }

  /**
   * Convert the root field to a java map object explicitly mapping the columns defined (only non nested objects)
   * @param record record to be converted
   * @return Java row representation for the record
   */
  Map<String, Object> covertToRowObjectFromRecordExplicitly(
      Record record,
      List<BigQueryFieldMappingConfig> fieldMappingConfigs,
      boolean ignoreInvalidColumn
  ) throws OnRecordErrorException {
    Map<String, Object> rowObject = new LinkedHashMap<>();
    for (BigQueryFieldMappingConfig mappingConfig : fieldMappingConfigs) {
      if (!record.has(mappingConfig.fieldPath)) {
        if (!ignoreInvalidColumn) {
          throw new OnRecordErrorException(record, Errors.BIGQUERY_13, mappingConfig.fieldPath);
        }
      } else {
        rowObject.put(mappingConfig.columnName, record.get(mappingConfig.fieldPath).getValue());
      }
    }
    return rowObject;
  }
}
