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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Errors;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BigQueryTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTarget.class);
  private static final Joiner COMMA_JOINER = Joiner.on(",");

  static final String YYYY_MM_DD = "yyyy-MM-dd";
  static final String HH_MM_SS_SSSSSS = "HH:mm:ss.SSSSSS";
  static final String YYYY_MM_DD_T_HH_MM_SS_SSSSSS = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";

  private final SimpleDateFormat dateFormat;
  private final SimpleDateFormat timeFormat;
  private final SimpleDateFormat dateTimeFormat;

  private final BigQueryTargetConfig conf;

  private BigQuery bigQuery;
  private ELEval dataSetEval;
  private ELEval tableNameELEval;
  private ELEval rowIdELEval;
  private LoadingCache<TableId, Boolean> tableIdExistsCache;
  private ErrorRecordHandler errorRecordHandler;

  BigQueryTarget(BigQueryTargetConfig conf) {
    this.conf = conf;
    this.dateFormat = createSimpleDateFormat(YYYY_MM_DD);
    this.timeFormat  = createSimpleDateFormat(HH_MM_SS_SSSSSS);
    this.dateTimeFormat = createSimpleDateFormat(YYYY_MM_DD_T_HH_MM_SS_SSSSSS);
  }

  static SimpleDateFormat createSimpleDateFormat(String pattern) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return simpleDateFormat;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.credentials.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      if (issues.isEmpty()) {
        try {
          Optional.ofNullable(provider.getCredentials()).ifPresent(credentials ->
              bigQuery
                  = BigQueryDelegate.getBigquery(credentials, conf.credentials.getProjectId()));
        } catch (IOException e) {
          LOG.error(Errors.BIGQUERY_05.getMessage(), e);
          issues.add(getContext().createConfigIssue(Groups.CREDENTIALS.name(),
              "conf.credentials.connection.credentialsProvider",
              Errors.BIGQUERY_05
          ));
        }
      }
    });

    dataSetEval = getContext().createELEval("datasetEL");
    tableNameELEval = getContext().createELEval("tableNameEL");
    rowIdELEval = getContext().createELEval("rowIdExpression");

    CacheBuilder tableIdExistsCacheBuilder = CacheBuilder.newBuilder();
    if (conf.maxCacheSize != -1) {
      tableIdExistsCacheBuilder.maximumSize(conf.maxCacheSize);
    }

    tableIdExistsCache = tableIdExistsCacheBuilder.build(new CacheLoader<TableId, Boolean>() {
      @Override
      public Boolean load(TableId key) throws Exception {
        return bigQuery.getTable(key) != null;
      }
    });
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Map<TableId, List<Record>> tableIdToRecords = new LinkedHashMap<>();
    Map<Long, Record> requestIndexToRecords = new LinkedHashMap<>();

    if (batch.getRecords().hasNext()) {
      ELVars elVars = getContext().createELVars();
      batch.getRecords().forEachRemaining(record -> {
        RecordEL.setRecordInContext(elVars, record);
        try {
          String datasetName = dataSetEval.eval(elVars, conf.datasetEL, String.class);
          String tableName = tableNameELEval.eval(elVars, conf.tableNameEL, String.class);
          TableId tableId = TableId.of(datasetName, tableName);
          if(!tableIdExistsCache.get(tableId)) {
            errorRecordHandler.onError(new OnRecordErrorException(
                record,
                Errors.BIGQUERY_17,
                datasetName,
                tableName,
                conf.credentials.getProjectId()
            ));
          }
          else {
            List<Record> tableIdRecords = tableIdToRecords.computeIfAbsent(tableId, t -> new ArrayList<>());
            tableIdRecords.add(record);
          }
        } catch (ELEvalException e) {
          LOG.error("Error evaluating DataSet/TableName EL", e);
          errorRecordHandler.onError(new OnRecordErrorException(
              record,
              Errors.BIGQUERY_10,
              e
          ));
        } catch (ExecutionException e){
          LOG.error("Error when checking exists for tableId, Reason : {}", e.getMessage(), e);
          Throwable rootCause = Throwables.getRootCause(e);
          errorRecordHandler.onError(new OnRecordErrorException(
              record,
              Errors.BIGQUERY_13,
              rootCause
          ));

        } catch (IllegalArgumentException | BigQueryException | UncheckedExecutionException e) {
          errorRecordHandler.onError(new OnRecordErrorException(
              record,
              Errors.BIGQUERY_18,
              e.getMessage()
          ));
        }
      });

      tableIdToRecords.forEach((tableId, records) -> {
        final AtomicLong index = new AtomicLong(0);
        final AtomicBoolean areThereRecordsToWrite = new AtomicBoolean(false);
        InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
        records.forEach(record -> {
              try {
                String insertId  = getInsertIdForRecord(elVars, record);
                Map<String, ?> rowContent = convertToRowObjectFromRecord(record);
                if (rowContent.isEmpty()) {
                  throw new OnRecordErrorException(record, Errors.BIGQUERY_14);
                }
                insertAllRequestBuilder.addRow(insertId, rowContent);
                areThereRecordsToWrite.set(true);
                requestIndexToRecords.put(index.getAndIncrement(), record);
              } catch (OnRecordErrorException e) {
                LOG.error(
                    "Error when converting record {} to row, Reason : {} ",
                    record.getHeader().getSourceId(),
                    e.getMessage()
                );
                errorRecordHandler.onError(e);
              }
            }
        );

        if (areThereRecordsToWrite.get()) {
          insertAllRequestBuilder.setIgnoreUnknownValues(conf.ignoreInvalidColumn);
          insertAllRequestBuilder.setSkipInvalidRows(false);

          InsertAllRequest request = insertAllRequestBuilder.build();

          if (!request.getRows().isEmpty()) {
            try {
              InsertAllResponse response = bigQuery.insertAll(request);
              if (response.hasErrors()) {
                response.getInsertErrors().forEach((requestIdx, errors) -> {
                  Record record = requestIndexToRecords.get(requestIdx);
                  String messages = COMMA_JOINER.join(
                      errors.stream()
                          .map(BigQueryError::getMessage)
                          .collect(Collectors.toList())
                  );
                  String reasons = COMMA_JOINER.join(
                      errors.stream()
                          .map(BigQueryError::getReason)
                          .collect(Collectors.toList())
                  );
                  LOG.error(
                      "Error when inserting record {}, Reasons : {}, Messages : {}",
                      record.getHeader().getSourceId(),
                      reasons,
                      messages
                  );
                  errorRecordHandler.onError(new OnRecordErrorException(
                      record,
                      Errors.BIGQUERY_11,
                      reasons,
                      messages
                  ));
                });
              }
            } catch (BigQueryException e) {
              LOG.error(Errors.BIGQUERY_13.getMessage(), e);
              //Put all records to error.
              for (long i = 0; i < request.getRows().size(); i++) {
                Record record = requestIndexToRecords.get(i);
                errorRecordHandler.onError(new OnRecordErrorException(
                    record,
                    Errors.BIGQUERY_13,
                    e
                ));
              }
            }
          }
        }
      });
    }
  }

  /**
   * Evaluate and obtain the row id if the expression is present or return null.
   */
  private String getInsertIdForRecord(ELVars elVars, Record record) throws OnRecordErrorException {
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
  private Map<String, Object> convertToRowObjectFromRecord(Record record) throws OnRecordErrorException {
    Field rootField = record.get();
    Map<String, Object> rowObject = new LinkedHashMap<>();
    if (rootField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      Map<String, Field> fieldMap = rootField.getValueAsMap();
      for (Map.Entry<String, Field> fieldEntry : fieldMap.entrySet()) {
        Field field = fieldEntry.getValue();
        //Skip null value fields
        if (field.getValue() != null){
          try {
            rowObject.put(fieldEntry.getKey(), getValueFromField("/" + fieldEntry.getKey(), field));
          } catch (IllegalArgumentException e) {
            throw new OnRecordErrorException(record, Errors.BIGQUERY_13, e.getMessage());
          }
        }
      }
    } else {
      throw new OnRecordErrorException(record,  Errors.BIGQUERY_16);
    }
    return rowObject;
  }

  /**
   * Convert the sdc Field to an object for row content
   */
  private Object getValueFromField(String fieldPath, Field field) {
    LOG.trace("Visiting Field Path '{}' of type '{}'", fieldPath, field.getType());
    switch (field.getType()) {
      case LIST:
        //REPEATED
        List<Field> listField = field.getValueAsList();
        //Convert the list to map with indices as key and Field as value (Map<Integer, Field>)
        Map<Integer, Field> fields =
            IntStream.range(0, listField.size()).boxed()
                .collect(Collectors.toMap(Function.identity(), listField::get));
        //filter map to remove fields with null value
        fields = fields.entrySet().stream()
            .filter(e -> e.getValue().getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        //now use the map index to generate field path and generate object for big query write
        return fields.entrySet().stream()
            .map(e -> getValueFromField(fieldPath + "[" + e.getKey() + "]", e.getValue()))
            .collect(Collectors.toList());
      case MAP:
      case LIST_MAP:
        //RECORD
        return field.getValueAsMap().entrySet().stream()
            .filter(me -> me.getValue().getValue() != null)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> getValueFromField(fieldPath + "/" + e.getKey(), e.getValue())
                )
            );
      case DATE:
        return dateFormat.format(field.getValueAsDate());
      case TIME:
        return timeFormat.format(field.getValueAsTime());
      case DATETIME:
        return dateTimeFormat.format(field.getValueAsDatetime());
      case BYTE_ARRAY:
        return Base64.getEncoder().encodeToString(field.getValueAsByteArray());
      case DECIMAL:
      case BYTE:
      case CHAR:
      case FILE_REF:
        throw new IllegalArgumentException(Utils.format(Errors.BIGQUERY_12.getMessage(), fieldPath, field.getType()));
      default:
        //Boolean -> Map to Boolean in big query
        //Float, Double -> Map to Float in big query
        //String -> maps to String in big query
        //Short, Integer, Long -> Map to integer in big query
        return field.getValue();
    }
  }
}
