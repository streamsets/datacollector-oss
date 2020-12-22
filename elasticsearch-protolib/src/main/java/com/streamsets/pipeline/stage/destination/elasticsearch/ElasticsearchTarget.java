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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.elasticsearch.ElasticsearchStageDelegate;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchTargetConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public class ElasticsearchTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchTarget.class);
  private final ElasticsearchTargetConfig conf;
  private ELEval timeDriverEval;
  private TimeZone timeZone;
  private Date batchTime;
  private ELEval indexEval;
  private ELEval typeEval;
  private ELEval docIdEval;
  private ELEval parentIdEval;
  private ELEval routingEval;
  private ELEval additionalPropertiesEval;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private ElasticsearchStageDelegate delegate;
  private static final Pattern elVarPattern = Pattern.compile(".*\\$\\{.*:.*\\(.*\\)\\}.*");
  private String additionalProperties;
  private boolean additionalPropertiesIsEval;

  public ElasticsearchTarget(ElasticsearchTargetConfig conf) {
    this.conf = conf;
    if (this.conf.params == null) {
      this.conf.params = new HashMap<>();
    }
    this.timeZone = TimeZone.getTimeZone(ZoneId.of(conf.timeZoneID));
  }

  private void validateEL(ELEval elEval, String elStr, String config, ErrorCode parseError, ErrorCode evalError,
      List<ConfigIssue> issues) {
    ELVars vars = getContext().createELVars();
    RecordEL.setRecordInContext(vars, getContext().createRecord("validateConfigs"));
    TimeEL.setCalendarInContext(vars, Calendar.getInstance());
    try {
      getContext().parseEL(elStr);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), config, parseError, ex.toString(), ex));
      return;
    }
    try {
      elEval.eval(vars, elStr, String.class);
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), config, evalError, ex.toString(), ex));
    }
  }

  /**
   * This method analyze the Additional Properties for recognize record-labels and modify it to a correct format for
   * JSON conversion. Adds "in front of and behind the every record-label.
   */
  private String validateAdditionalProperties(String additionalProperties) {
    additionalProperties = additionalProperties.replace("${record:", "\"${record:");
    additionalProperties = additionalProperties.replace(")}", ")}\"");
    return additionalProperties;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext(), getContext());

    indexEval = getContext().createELEval("indexTemplate");
    typeEval = getContext().createELEval("typeTemplate");
    docIdEval = getContext().createELEval("docIdTemplate");
    parentIdEval = getContext().createELEval("parentIdTemplate");
    routingEval = getContext().createELEval("routingTemplate");
    timeDriverEval = getContext().createELEval("timeDriver");
    additionalPropertiesEval = getContext().createELEval("rawAdditionalProperties");

    try {
      getRecordTime(getContext().createRecord("validateTimeDriver"));
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          "timeDriverEval",
          Errors.ELASTICSEARCH_18,
          ex.getMessage(),
          ex
      ));
    }

    validateEL(
        indexEval,
        conf.indexTemplate,
        "elasticSearchConfig.indexTemplate",
        Errors.ELASTICSEARCH_00,
        Errors.ELASTICSEARCH_01,
        issues
    );
    validateEL(
        typeEval,
        conf.typeTemplate,
        "elasticSearchConfig.typeTemplate",
        Errors.ELASTICSEARCH_02,
        Errors.ELASTICSEARCH_03,
        issues
    );
    if (!StringUtils.isEmpty(conf.docIdTemplate)) {
      validateEL(
          typeEval,
          conf.docIdTemplate,
          "elasticSearchConfig.docIdTemplate",
          Errors.ELASTICSEARCH_04,
          Errors.ELASTICSEARCH_05,
          issues
      );
    } else {
      if (conf.defaultOperation != ElasticsearchOperationType.INDEX) {
        issues.add(
            getContext().createConfigIssue(
                ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
                "elasticSearchConfig.docIdTemplate",
                Errors.ELASTICSEARCH_19,
                conf.defaultOperation.getLabel()
            )
        );
      }
    }
    if (!StringUtils.isEmpty(conf.parentIdTemplate)) {
      validateEL(
          typeEval,
          conf.parentIdTemplate,
          "elasticSearchConfig.parentIdTemplate",
          Errors.ELASTICSEARCH_27,
          Errors.ELASTICSEARCH_28,
          issues
      );
    }
    if (!StringUtils.isEmpty(conf.routingTemplate)) {
      validateEL(
          typeEval,
          conf.routingTemplate,
          "elasticSearchConfig.routingTemplate",
          Errors.ELASTICSEARCH_29,
          Errors.ELASTICSEARCH_30,
          issues
      );
    }
    additionalPropertiesIsEval = elVarPattern.matcher(conf.rawAdditionalProperties).matches();
    if (additionalPropertiesIsEval) {
      validateEL(additionalPropertiesEval,
          conf.rawAdditionalProperties,
          "elasticSearchConfig.rawAdditionalProperties",
          Errors.ELASTICSEARCH_36,
          Errors.ELASTICSEARCH_37,
          issues
      );
      additionalProperties = validateAdditionalProperties(conf.rawAdditionalProperties);
    } else {
      additionalProperties = conf.rawAdditionalProperties;
    }

    try{
      // try to create JSONObject from input, validation issue if it fails.
      JsonParser parser = new JsonParser();
      parser.parse(additionalProperties).getAsJsonObject();
    }catch (Exception e){
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          "rawAdditionalProperties",
          Errors.ELASTICSEARCH_34,
          additionalProperties,
          Optional.ofNullable(e.getMessage()).orElse("no details provided"),
          e
      ));
    }

    delegate = new ElasticsearchStageDelegate(getContext(), conf);

    issues = delegate.init("elasticSearchConfig", issues);

    generatorFactory = new DataGeneratorFactoryBuilder(getContext(), DataGeneratorFormat.JSON)
        .setMode(Mode.MULTIPLE_OBJECTS)
        .setCharset(Charset.forName(conf.charset))
        .build();

    return issues;
  }

  @Override
  public void destroy() {
    if(delegate != null) {
      delegate.destroy();
    }
    super.destroy();
  }

  @VisibleForTesting
  Date getRecordTime(Record record) throws ELEvalException {
    ELVars variables = getContext().createELVars();
    TimeNowEL.setTimeNowInContext(variables, getBatchTime());
    RecordEL.setRecordInContext(variables, record);
    return timeDriverEval.eval(variables, conf.timeDriver, Date.class);
  }

  @VisibleForTesting
  String getRecordIndex(ELVars elVars, Record record) throws ELEvalException {
    Date date = getRecordTime(record);
    if (date != null) {
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(date);
      TimeEL.setCalendarInContext(elVars, calendar);
    }
    return indexEval.eval(elVars, conf.indexTemplate, String.class);
  }

  @Override
  public void write(final Batch batch) throws StageException {
    setBatchTime();
    ELVars elVars = getContext().createELVars();
    TimeNowEL.setTimeNowInContext(elVars, getBatchTime());
    Iterator<Record> it = batch.getRecords();

    StringBuilder bulkRequest = new StringBuilder();

    //we need to keep the records in order of appearance in case we have indexing errors
    //and error handling is TO_ERROR
    List<Record> records = new ArrayList<>();

    while (it.hasNext()) {
      Record record = it.next();
      records.add(record);

      try {
        RecordEL.setRecordInContext(elVars, record);
        String index = getRecordIndex(elVars, record);
        String type = typeEval.eval(elVars, conf.typeTemplate, String.class);
        String id = null;
        if (!StringUtils.isEmpty(conf.docIdTemplate)) {
          id = docIdEval.eval(elVars, conf.docIdTemplate, String.class);
        }
        String parent = null;
        if (!StringUtils.isEmpty(conf.parentIdTemplate)) {
          parent = parentIdEval.eval(elVars, conf.parentIdTemplate, String.class);
        }
        String routing = null;
        if (!StringUtils.isEmpty(conf.routingTemplate)) {
          routing = routingEval.eval(elVars, conf.routingTemplate, String.class);
        }
        String additionalPropertiesName = null;
        if (additionalPropertiesIsEval) {
          additionalPropertiesName = additionalPropertiesEval.eval(elVars, additionalProperties, String.class);
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();

        int opCode = -1;
        String opType = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
        String recordJson = new String(baos.toByteArray(), StandardCharsets.UTF_8).replace("\n", "");
        // Check if the operation code from header attribute is valid
        if (!StringUtils.isEmpty(opType)) {
          try {
            opCode = ElasticsearchOperationType.convertToIntCode(opType);
          } catch (NumberFormatException | UnsupportedOperationException ex) {
            // Operation obtained from header is not supported. Handle accordingly
            switch (conf.unsupportedAction) {
              case DISCARD:
                LOG.debug("Discarding record with unsupported operation {}", opType);
                break;
              case SEND_TO_ERROR:
                errorRecordHandler.onError(new OnRecordErrorException(
                    record,
                    Errors.ELASTICSEARCH_13,
                    ex.getMessage(),
                    ex
                ));
                break;
              case USE_DEFAULT:
                opCode = conf.defaultOperation.code;
                break;
              default: //unknown action
                errorRecordHandler.onError(new OnRecordErrorException(
                    record,
                    Errors.ELASTICSEARCH_14,
                    ex.getMessage(),
                    ex
                ));
            }
          }
        } else {
          // No header attribute set. Use default.
          opCode = conf.defaultOperation.code;
        }
        bulkRequest.append(getOperation(index, type, id, parent, routing, additionalPropertiesName, recordJson, opCode));
      } catch (IOException ex) {
        errorRecordHandler.onError(new OnRecordErrorException(record,
            Errors.ELASTICSEARCH_15,
            record.getHeader().getSourceId(),
            Optional.ofNullable(ex.getMessage()).orElse("no details provided"),
            ex
        ));
      }
    }

    if (!records.isEmpty()) {
      try {
        HttpEntity entity = new StringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
        Response response;
        response = delegate.performRequest(
            "POST",
            "/_bulk",
            conf.params,
            entity,
            delegate.getAuthenticationHeader(conf.connection.securityConfig.securityUser.get(),
                conf.connection.securityConfig.securityPassword.get())
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        response.getEntity().writeTo(baos);
        JsonObject json = new JsonParser().parse(baos.toString()).getAsJsonObject();
        baos.close();

        // Handle errors in bulk requests individually.
        boolean errors = json.get("errors").getAsBoolean();
        if (errors) {
          List<ErrorItem> errorItems;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              errorItems = extractErrorItems(json);
              for (ErrorItem item : errorItems) {
                Record record = records.get(item.index);
                getContext().toError(record, Errors.ELASTICSEARCH_16, record.getHeader().getSourceId(), item.reason);
              }
              break;
            case STOP_PIPELINE:
              errorItems = extractErrorItems(json);
              throw new StageException(Errors.ELASTICSEARCH_17, errorItems.size(), "one or more operations failed");
            default:
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord()));
          }
        }
      } catch (IOException ex) {
        errorRecordHandler.onError(records, new StageException(
            Errors.ELASTICSEARCH_17,
            records.size(),
            Optional.ofNullable(ex.getMessage()).orElse("no details provided"),
            ex
        ));
      }
    }
  }

  Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  Date getBatchTime() {
    return batchTime;
  }

  private String getOperation( String index, String type, String id, String parent, String routing,
      String additionalProperties, String record, int opCode) {
    StringBuilder op = new StringBuilder();
    switch (opCode) {
      case OperationType.UPSERT_CODE:
        getOperationMetadata("index", index, type, id, parent, routing, additionalProperties, op);
        op.append(String.format("%s%n", record));
        break;
      case OperationType.INSERT_CODE:
        getOperationMetadata("create", index, type, id, parent, routing, additionalProperties, op);
        op.append(String.format("%s%n", record));
        break;
      case OperationType.UPDATE_CODE:
        getOperationMetadata("update", index, type, id, parent, routing, additionalProperties, op);
        op.append(String.format("{\"doc\":%s}%n", record));
        break;
      case OperationType.MERGE_CODE:
        getOperationMetadata("update", index, type, id, parent, routing, additionalProperties, op);
        op.append(String.format("{\"doc_as_upsert\": \"true\", \"doc\":%s}%n", record));
        break;
      case OperationType.DELETE_CODE:
        getOperationMetadata("delete", index, type, id, parent, routing, additionalProperties, op);
        break;
      default:
        LOG.error("Operation {} not supported", opCode);
        throw new UnsupportedOperationException(String.format("Unsupported Operation: %s", opCode));
    }
    return op.toString();
  }

  private void getOperationMetadata( String operation, String index, String type, String id, String parent,
      String routing, String additionalPropertiesValue, StringBuilder sb) {
    sb.append(String.format("{\"%s\":{\"_index\":\"%s\",\"_type\":\"%s\"", operation, index, type));
    if (!StringUtils.isEmpty(id)) {
      sb.append(String.format(",\"_id\":\"%s\"", id));
    }
    if (!StringUtils.isEmpty(parent)) {
      sb.append(String.format(",\"parent\":\"%s\"", parent));
    }
    if (!StringUtils.isEmpty(routing)) {
      sb.append(String.format(",\"routing\":\"%s\"", routing));
    }
    // Add additional properties from JSON editor.
    if (!StringUtils.isEmpty(additionalPropertiesValue)) {
      String additionalProperties = addAdditionalProperties(additionalPropertiesValue);
      sb.append(additionalProperties);
    }
    sb.append(String.format("}}%n"));
  }

  @VisibleForTesting
  String addAdditionalProperties(String additionalProperties) {
    JsonParser parser = new JsonParser();
    JsonObject additionalPropertiesAsJson = parser.parse(additionalProperties).getAsJsonObject();

    StringBuilder sb = new StringBuilder();
    //Create a String appending all the input properties
    additionalPropertiesAsJson.entrySet().forEach(e -> sb.append(String.format(",\"%s\":%s", e.getKey(), e.getValue())));

    return sb.toString();
  }

  private List<ErrorItem> extractErrorItems(JsonObject json) {
    List<ErrorItem> errorItems = new ArrayList<>();
    JsonArray items = json.getAsJsonArray("items");
    for (int i = 0; i < items.size(); i++) {
      JsonObject item = items.get(i).getAsJsonObject().entrySet().iterator().next().getValue().getAsJsonObject();
      int status = item.get("status").getAsInt();
      if (status >= 400) {
        Object error = item.get("error");
        // In some old versions, "error" is a simple string not a json object.
        if (error instanceof JsonObject) {
          errorItems.add(new ErrorItem(i, item.getAsJsonObject("error").get("reason").getAsString()));
        } else if (error instanceof JsonPrimitive) {
          errorItems.add(new ErrorItem(i, item.getAsJsonPrimitive("error").getAsString()));
        } else {
          // Error would be null if json has no "error" field.
          errorItems.add(new ErrorItem(i, ""));
        }
      }
    }
    return errorItems;
  }

  private static class ErrorItem {
    int index;
    String reason;
    ErrorItem(int index, String reason) {
      this.index = index;
      this.reason = reason;
    }
  }
}
