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
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BigQueryTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTarget.class);
  private static final Joiner COMMA_JOINER = Joiner.on(",");

  static final SimpleDateFormat DATE_FORMAT = createSimpleDateFormat("yyyy-MM-dd");
  static final SimpleDateFormat TIME_FORMAT = createSimpleDateFormat("HH:mm:ss.SSSSSS");
  static final SimpleDateFormat DATE_TIME_FORMAT = createSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  private final BigQueryTargetConfig conf;

  private BigQuery bigQuery;
  private ELEval dataSetEval;
  private ELEval tableNameELEval;
  private ELEval rowIdELEval;


  BigQueryTarget(BigQueryTargetConfig conf) {
    this.conf = conf;
  }

  private static SimpleDateFormat createSimpleDateFormat(String pattern) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return simpleDateFormat;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.credentials.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      if (issues.isEmpty()) {
        try {
          Optional.ofNullable(provider.getCredentials()).ifPresent(c -> bigQuery = BigQueryDelegate.getBigquery(c, conf.credentials.projectId));
        } catch (IOException e) {
          LOG.error(Errors.BIGQUERY_05.getMessage(), e);
          issues.add(getContext().createConfigIssue(
              Groups.CREDENTIALS.name(),
              "conf.credentials.credentialsProvider",
              Errors.BIGQUERY_05
          ));
        }
      }
    });

    dataSetEval = getContext().createELEval("datasetEL");
    tableNameELEval = getContext().createELEval("tableNameEL");
    rowIdELEval = getContext().createELEval("rowIdExpression");
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Map<TableId, List<Record>> tableIdToRecords = new LinkedHashMap<>();
    Map<Long, Record> indexToRecords = new LinkedHashMap<>();
    final AtomicBoolean areThereRecordsToWrite = new AtomicBoolean(false);

    if (batch.getRecords().hasNext()) {
      final AtomicLong index = new AtomicLong(0);
      ELVars elVars = getContext().createELVars();
      batch.getRecords().forEachRemaining(record -> {
        indexToRecords.put(index.getAndIncrement(), record);
        RecordEL.setRecordInContext(elVars, record);
        try {
          String datasetName = dataSetEval.eval(elVars, conf.datasetEL, String.class);
          String tableName = tableNameELEval.eval(elVars, conf.tableNameEL, String.class);
          TableId tableId = TableId.of(datasetName, tableName);
          List<Record> tableIdRecords = tableIdToRecords.computeIfAbsent(tableId, t -> new ArrayList<>());
          tableIdRecords.add(record);
        } catch (ELEvalException e) {
          LOG.error("Error evaluating DataSet/TableName EL", e);
          getContext().toError(record, Errors.BIGQUERY_10, e);
        }
      });

      tableIdToRecords.forEach((tableId, records) -> {
        InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
        records.forEach(record -> {
              try {
                String recordId  = getRowIdForRecord(elVars, record);
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
                insertAllRequestBuilder.addRow(recordId, rowContent);
                areThereRecordsToWrite.set(true);
              } catch (OnRecordErrorException e) {
                LOG.error("Error when converting record {} to row, Reason : {} ", record.getHeader().getSourceId(), e.getMessage());
                getContext().toError(record, e.getErrorCode(), e.getParams());
              }
            }
        );

        if (areThereRecordsToWrite.get()) {
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
        }
      });
    }
  }

  /**
   * Evaluate and obtain the row id if the expression is present or return null.
   */
  private String getRowIdForRecord(ELVars elVars, Record record) throws OnRecordErrorException {
    String recordId = null;
    RecordEL.setRecordInContext(elVars, record);
    try {
      if (!(StringUtils.isEmpty(conf.rowIdExpression))) {
        recordId = rowIdELEval.eval(elVars, conf.rowIdExpression, String.class);
        if (StringUtils.isEmpty(recordId)) {
          throw new OnRecordErrorException(record, Errors.BIGQUERY_15);
        }
      }
    } catch (ELEvalException e) {
      LOG.error("Error evaluating Row Expression EL", e);
      throw new OnRecordErrorException(record, Errors.BIGQUERY_10,e);
    }
    return recordId;
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
        //Skip null value fields
        Optional.ofNullable(field.getValue()).ifPresent(v -> rowObject.put(fieldEntry.getKey(), getValueFromField(field)));
      }
    } else {
      throw new OnRecordErrorException(record,  Errors.BIGQUERY_16);
    }
    return rowObject;
  }

  /**
   * Convert the root field to a java map object explicitly mapping the columns defined (only non nested objects)
   * @param record record to be converted
   * @return Java row representation for the record
   */
  private Map<String, Object> covertToRowObjectFromRecordExplicitly(
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
        Field field = record.get(mappingConfig.fieldPath);
        //Skip null value fields
        Optional.ofNullable(field.getValue()).ifPresent( v -> rowObject.put(mappingConfig.columnName, getValueFromField(field)));
      }
    }
    return rowObject;
  }

  /**
   * Convert the sdc Field to an object for row content
   */
  private Object getValueFromField(Field field) {
    switch (field.getType()) {
      case LIST:
        //REPEATED
        return field.getValueAsList().stream().filter(le -> le.getValue() != null).map(this::getValueFromField).collect(Collectors.toList());
      case MAP:
      case LIST_MAP:
        //RECORD
        return field.getValueAsMap().entrySet().stream().filter(me -> me.getValue().getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> getValueFromField(e.getValue())));
      case DATE:
        return DATE_FORMAT.format(field.getValueAsDate());
      case TIME:
        return TIME_FORMAT.format(field.getValueAsTime());
      case DATETIME:
        return DATE_TIME_FORMAT.format(field.getValueAsDatetime());
      case BYTE_ARRAY:
        return Base64.getEncoder().encodeToString(field.getValueAsByteArray());
      //TODO: SDC-7293 -> Throw errors for decimal and other unsupported types
      default:
        return field.getValue();
    }
  }
}
