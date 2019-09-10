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
package com.streamsets.pipeline.stage.destination.solr;


import com.esotericsoftware.minlog.Log;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.solr.api.Errors;
import com.streamsets.pipeline.solr.api.SdcSolrTarget;
import com.streamsets.pipeline.solr.api.SdcSolrTargetFactory;
import com.streamsets.pipeline.solr.api.TargetFactorySettings;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SolrTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(SolrTarget.class);

  private final SolrConfigBean conf;

  private ErrorRecordHandler errorRecordHandler;
  private SdcSolrTarget sdcSolrTarget;

  // Solr schema fields are the union of three disjoint lists: required, optional and auto-generated fields. The
  // auto-generated list contains those fields that Solr can generate when not provided by the client.
  private List<String> requiredFieldNamesMap;
  private List<String> optionalFieldNamesMap;

  public SolrTarget(final SolrConfigBean conf) {
    this.conf = conf;
  }

  protected ConfigIssue createSolrConfigIssue(String configName, ErrorCode errorMessage, Object... objects) {
    return getContext().createConfigIssue(
        Groups.SOLR.name(),
        configName,
        errorMessage,
        objects
    );
  }

  protected void validateRecordSolrFieldsPath(List<ConfigIssue> issues) {
    if (StringUtils.isBlank(conf.recordSolrFieldsPath)) {
      issues.add(createSolrConfigIssue("conf.recordSolrFieldsPath", Errors.SOLR_11));
    }
  }

  protected boolean inSingleNode() {
    return SolrInstanceType.SINGLE_NODE.equals(conf.instanceType.getInstanceType());
  }

  protected boolean inCloud() {
    return SolrInstanceType.SOLR_CLOUD.equals(conf.instanceType.getInstanceType());
  }

  protected boolean validateSolrURI(List<ConfigIssue> issues) {
    boolean solrInstanceInfo = true;
    if((conf.solrURI == null || conf.solrURI.isEmpty()) && inSingleNode()) {
      solrInstanceInfo = false;
      issues.add(createSolrConfigIssue("conf.solrURI", Errors.SOLR_00));
    } else if((conf.zookeeperConnect == null || conf.zookeeperConnect.isEmpty()) && inCloud()) {
      solrInstanceInfo = false;
      issues.add(createSolrConfigIssue("conf.zookeeperConnect", Errors.SOLR_01));
    }
    return solrInstanceInfo;
  }

  protected void validateFieldsNamesMap(List<ConfigIssue> issues) {
    if (conf.fieldNamesMap == null || conf.fieldNamesMap.isEmpty() &&
      !conf.fieldsAlreadyMappedInRecord) {
    issues.add(createSolrConfigIssue("conf.fieldNamesMap", Errors.SOLR_02));
    }
  }

  protected void validateConnectionTimeout(List<ConfigIssue> issues) {
    if (conf.connectionTimeout < 0) {
      issues.add(createSolrConfigIssue("conf.connectionTimeout", Errors.SOLR_14));
    }
  }

  protected void validateSocketConnection(List<ConfigIssue> issues) {
    if (conf.socketTimeout < 0) {
      issues.add(createSolrConfigIssue("conf.socketTimeout", Errors.SOLR_15));
    }
  }

  protected boolean validateConfigs(List<ConfigIssue> issues) {
    validateRecordSolrFieldsPath(issues);
    boolean solrInstanceInfo = validateSolrURI(issues);
    validateFieldsNamesMap(issues);
    validateConnectionTimeout(issues);
    validateSocketConnection(issues);
    return solrInstanceInfo;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    boolean solrInstanceInfo = validateConfigs(issues);

    if (solrInstanceInfo) {
      TargetFactorySettings settings = new TargetFactorySettings(
          conf.instanceType.toString(),
          conf.solrURI,
          conf.zookeeperConnect,
          conf.defaultCollection,
          conf.kerberosAuth,
          conf.skipValidation,
          conf.waitFlush,
          conf.waitSearcher,
          conf.softCommit,
          conf.ignoreOptionalFields,
          conf.connectionTimeout,
          conf.socketTimeout
      );
      sdcSolrTarget = SdcSolrTargetFactory.create(settings).create();
      try {
        // Retrieve required and optional fields from the Solr schema.
        sdcSolrTarget.init();
        this.requiredFieldNamesMap = filterAutogeneratedFieldNames(sdcSolrTarget.getRequiredFieldNamesMap());
        if (!conf.ignoreOptionalFields) {
          this.optionalFieldNamesMap = filterAutogeneratedFieldNames(sdcSolrTarget.getOptionalFieldNamesMap());
        }

        // If using a user-defined record to Solr fields mapping, check this mapping against Solr schema to detect
        // missing fields.
        if (!conf.fieldsAlreadyMappedInRecord) {
          List<String> missingFields = checkMissingFields(conf.fieldNamesMap, requiredFieldNamesMap);
          if (!missingFields.isEmpty()) {
            issues.add(createSolrConfigIssue("conf.fieldNamesMap", Errors.SOLR_12,
                Joiner.on(", ").join(missingFields)));
          }
          if (!conf.ignoreOptionalFields) {
            missingFields = checkMissingFields(conf.fieldNamesMap, optionalFieldNamesMap);
            if (!missingFields.isEmpty()) {
              issues.add(createSolrConfigIssue("conf.fieldNamesMap", Errors.SOLR_13,
                  Joiner.on(", ").join(missingFields)));
            }
          }
        }
      } catch (Exception ex) {
        String configName = "conf.solrURI";
        if(InstanceTypeOptions.SOLR_CLOUD.equals(conf.instanceType.getInstanceType())) {
          configName = "conf.zookeeperConnect";
        }
        issues.add(createSolrConfigIssue(configName, Errors.SOLR_03, ex.toString(), ex));
      }
    }

    return issues;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    List<Map<String, Object>> batchFieldMap = new ArrayList();
    List<Record> recordsBackup = new ArrayList<>();
    boolean atLeastOne = false;

    while (it.hasNext()) {
      atLeastOne = true;
      Record record = it.next();
      Map<String, Object> fieldMap = new HashMap();
      boolean correctRecord = true;
      if (conf.fieldsAlreadyMappedInRecord) {
        try {
          Field recordSolrFields = record.get(conf.recordSolrFieldsPath);
          if (recordSolrFields == null) {
            correctRecord = false;
            handleError(record, Errors.SOLR_10, conf.recordSolrFieldsPath);
          }

          Field.Type recordSolrFieldType = recordSolrFields.getType();
          Map<String, Field> recordFieldMap = null;

          if (recordSolrFieldType == Field.Type.MAP) {
            recordFieldMap = recordSolrFields.getValueAsMap();
          } else if (recordSolrFieldType == Field.Type.LIST_MAP) {
            recordFieldMap = recordSolrFields.getValueAsListMap();
          } else {
            correctRecord = false;
            handleError(record, Errors.SOLR_09, conf.recordSolrFieldsPath, recordSolrFields.getType().name());
          }

          if (correctRecord) {
            // for each record field check if it's in the solr required fields list and save it for later
            if (requiredFieldNamesMap != null && !requiredFieldNamesMap.isEmpty()) {
              correctRecord = checkRecordContainsSolrFields(
                  recordFieldMap,
                  record,
                  requiredFieldNamesMap,
                  Errors.SOLR_07
              );
            }

            // check record contain optional fields if needed
            if (!conf.ignoreOptionalFields) {
              if (optionalFieldNamesMap != null && !optionalFieldNamesMap.isEmpty()) {
                correctRecord = checkRecordContainsSolrFields(
                    recordFieldMap,
                    record,
                    optionalFieldNamesMap,
                    Errors.SOLR_08
                );
              }
            }

            if (correctRecord) {
              // add fields to fieldMap to later add document to Solr
              for (Map.Entry<String, Field> recordFieldMapEntry : recordFieldMap.entrySet()) {
                fieldMap.put(
                    recordFieldMapEntry.getKey(),
                    JsonUtil.fieldToJsonObject(record, recordFieldMapEntry.getValue())
                );
              }
            }
          }

          if (!correctRecord) {
            record = null;
          }
        } catch (OnRecordErrorException ex) {
          errorRecordHandler.onError(ex);
          record = null;
        }

      } else {
        try {
          Set<String> missingFields = new HashSet<>();
          for (SolrFieldMappingConfig fieldMapping : conf.fieldNamesMap) {
            Field field = record.get(fieldMapping.field);
            if (field == null) {
              // The missing field is considered a validation error if it cannot be auto-generated by Solr and
              // (a) is a required field or (b) is an optional field and "Ignore Optional Fields" is unchecked.
              if (conf.autogeneratedFields == null ||
                  !conf.autogeneratedFields.contains(fieldMapping.solrFieldName)) {
                if (requiredFieldNamesMap != null && requiredFieldNamesMap.contains(fieldMapping.solrFieldName)) {
                  missingFields.add(fieldMapping.solrFieldName);
                } else if (!conf.ignoreOptionalFields) {
                  // We consider here as optional any non-required and non-autogenerated field. This case covers fields
                  // declared in the Solr schema as optional (non-required) and any new mapping defined by the user
                  // which adds a new field in Solr (Solr schemaless mode).
                  missingFields.add(fieldMapping.solrFieldName);
                }
              }
            } else {
              fieldMap.put(fieldMapping.solrFieldName, JsonUtil.fieldToJsonObject(record, field));
            }
          }

          if (!missingFields.isEmpty()) {
            String errorParams = Joiner.on(",").join(missingFields);
            handleError(record, Errors.SOLR_06, errorParams);
            record = null;
          }
        } catch (OnRecordErrorException ex) {
          errorRecordHandler.onError(ex);
          record = null;
        }
      }

      if (record != null) {
        try {
          if (ProcessingMode.BATCH.equals(conf.indexingMode)) {
            batchFieldMap.add(fieldMap);
            recordsBackup.add(record);
          } else {
            sdcSolrTarget.add(fieldMap);
          }
        } catch (StageException ex) {
          sendOnRecordErrorExceptionToHandler(record, Errors.SOLR_04, ex);
        }
      }
    }

    if (atLeastOne) {
      try {
        if (ProcessingMode.BATCH.equals(conf.indexingMode)) {
          sdcSolrTarget.add(batchFieldMap);
        }
        sdcSolrTarget.commit();
      } catch (StageException ex) {
        try {
          errorRecordHandler.onError(recordsBackup, ex);
        } catch (StageException ex2) {
          errorRecordHandler.onError(recordsBackup, ex2);
        }
      }
    }
  }

  @Override
  public void destroy() {
    if(this.sdcSolrTarget != null){
      try {
        this.sdcSolrTarget.destroy();
      } catch (Exception e) {
        Log.error(e.toString());
      }
    }
    super.destroy();
  }

  /**
   * Checks whether the record contains solr fields in solrFieldsMap or not.
   *
   * @param recordFieldMap Fields contained in the reocrd
   * @param record The whole record
   * @param solrFieldsMap Solr fields that will be searched in record fields
   * @param errorToThrow error to use if missing Solr fields are detected
   * @return true if record contains all solr fields in solrFieldsMap otherwise false
   * @throws StageException Exception thrown by handleError method call
   */
  private boolean checkRecordContainsSolrFields(
      Map<String, Field> recordFieldMap,
      Record record,
      List<String> solrFieldsMap,
      Errors errorToThrow
  ) throws StageException {
    // for (Map.Entry<String, Field> recordFieldMapEntry : recordFieldMap.entrySet())
    List<String> fieldsFound = new ArrayList<>();

    recordFieldMap.keySet().forEach(recordFieldKey -> {
      if (solrFieldsMap.contains(recordFieldKey)) {
        fieldsFound.add(recordFieldKey);
      }
    });

    // if record does not contain solr fields then process error accordingly
    if (solrFieldsMap.size() != fieldsFound.size()) {
      Set<String> missingFields = new HashSet<>();
      solrFieldsMap.forEach(requiredField -> {
        if (!fieldsFound.contains(requiredField)) {
          missingFields.add(requiredField);
        }
      });

      handleError(record, errorToThrow, Joiner.on(",").join(missingFields));

      return false;
    }

    return true;
  }

  /**
   * Filter auto-generated fields from the list passed as argument.
   *
   * @param fieldNames List of Solr field names.
   * @return A copy of {@code fieldNames} with all the auto-generated fields removed.
   */
  private List<String> filterAutogeneratedFieldNames(List<String> fieldNames) {
    List<String> result = new ArrayList<>();
    fieldNames.forEach(name -> {
      if (!conf.autogeneratedFields.contains(name)) {
        result.add(name);
      }
    });
    return result;
  }

  /**
   * Check that the list of mappings in {@code mappings} covers all the fields in {@code solrFields}.
   *
   * @param mappings List of record field name to Solr field name mappings.
   * @param solrFields List of Solr field names to check.
   * @return A list with the missing Solr field names in {@code mappings} according to {@code solrFields}.
   */
  private List<String> checkMissingFields(List<SolrFieldMappingConfig> mappings, List<String> solrFields) {
    List<String> missingFields = new ArrayList<>(solrFields);

    for (SolrFieldMappingConfig map : mappings) {
      missingFields.remove(map.solrFieldName);
    }
    return missingFields;
  }

  /**
   * Handles an error that occurred when processing a record. The error can either be logged or thrown in an exception.
   *
   * @param record The record which was being processed
   * @param errorTemplate The error template to be thrown if required
   * @param errorMessage The error specific message
   * @throws StageException Exception thrown if missingFieldAction indicates to stop the pipeline or send the record
   * to error. If missingFieldAction value is unknown an exception is also thrown
   */
  private void handleError(Record record, Errors errorTemplate, String errorMessage) throws StageException {
    handleError(record, errorTemplate, new String[] {errorMessage});
  }

  /**
   * Handles an error that occurred when processing a record. The error can either be logged or thrown in an exception.
   *
   * @param record The record which was being processed
   * @param errorTemplate The error template to be thrown if required
   * @param errorArguments The error specific arguments
   * @throws StageException Exception thrown if missingFieldAction indicates to stop the pipeline or send the record
   * to error. If missingFieldAction value is unknown an exception is also thrown
   */
  private void handleError(Record record, Errors errorTemplate, String ... errorArguments) throws StageException {
    switch (conf.missingFieldAction) {
      case DISCARD:
        LOG.debug(errorTemplate.getMessage(), errorArguments);
        break;
      case STOP_PIPELINE:
        throw new StageException(errorTemplate, errorArguments);
      case TO_ERROR:
        throw new OnRecordErrorException(record, errorTemplate, errorArguments);
      default: //unknown operation
        LOG.debug("Sending record to error due to unknown operation {}", conf.missingFieldAction);
        throw new OnRecordErrorException(record, errorTemplate, errorArguments);
    }
  }

  /**
   * Send exception ex to errorRecordHandler in order to let the handler process it.
   *
   * @param record The record which was being processed when the exception was thrown
   * @param error The error template
   * @param ex The exception that was thrown
   * @throws StageException Exception thrown by the errorRecordHandler when handling the exception ex
   */
  private void sendOnRecordErrorExceptionToHandler(Record record, Errors error, StageException ex)
      throws StageException {
    errorRecordHandler.onError(new OnRecordErrorException(
        record,
        error,
        record.getHeader().getSourceId(),
        ex.toString(),
        ex
    ));
  }
}
