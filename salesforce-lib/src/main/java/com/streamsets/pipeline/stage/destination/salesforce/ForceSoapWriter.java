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
package com.streamsets.pipeline.stage.destination.salesforce;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UndeleteResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class ForceSoapWriter extends ForceWriter {
  // Max number of records allowed in a create() call
  // See https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_create.htm
  private static final int MAX_RECORDS_CREATE = 200;
  private static final Logger LOG = LoggerFactory.getLogger(ForceSoapWriter.class);
  private final PartnerConnection partnerConnection;

  public ForceSoapWriter(Map<String, String> fieldMappings, PartnerConnection partnerConnection) {
    super(fieldMappings);
    this.partnerConnection = partnerConnection;
  }

  private String[] sObjectsToIds(SObject[] sobjects) {
    String[] ids = new String[sobjects.length];
    for (int i = 0; i < sobjects.length; i++) {
      ids[i] = sobjects[i].getId();
    }
    return ids;
  }

  private void handleErrorRecord(
      SObject sobject,
      Error[] errors,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ){
    Record record = recordMap.get(sobject);
    StringBuilder errorString = new StringBuilder();
    for (Error error : errors) {
      if (errorString.length() > 0) {
        errorString.append("\n");
      }
      errorString.append(error.getMessage());
    }
    errorRecords.add(new OnRecordErrorException(record,
        Errors.FORCE_11,
        record.getHeader().getSourceId(),
        errorString.toString()
    ));
  }

  private void create(
      SObject[] recordArray,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ) throws ConnectionException {
    LOG.info("Writing {} records to Salesforce", recordArray.length);

    SaveResult[] saveResults = partnerConnection.create(recordArray);

    LOG.info("{} records written to Salesforce", saveResults.length);

    // check the returned results for any errors
    for (int i = 0; i < saveResults.length; i++) {
      if (!saveResults[i].isSuccess()) {
        handleErrorRecord(recordArray[i], saveResults[i].getErrors(), recordMap, errorRecords);
      }
    }
  }

  private void update(
      SObject[] recordArray,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ) throws ConnectionException {
    LOG.info("Updating {} records in Salesforce", recordArray.length);

    SaveResult[] saveResults = partnerConnection.update(recordArray);

    LOG.info("{} records updated in Salesforce", saveResults.length);

    // check the returned results for any errors
    for (int i = 0; i < saveResults.length; i++) {
      if (!saveResults[i].isSuccess()) {
        handleErrorRecord(recordArray[i], saveResults[i].getErrors(), recordMap, errorRecords);
      }
    }
  }

  private void upsert(
      ForceTarget target,
      SObject[] recordArray,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ) throws ConnectionException {
    // Partition by ExternalIdField
    Multimap<String, SObject> partitions = ArrayListMultimap.create();

    for (SObject sobject : recordArray) {
      Record record = recordMap.get(sobject);
      RecordEL.setRecordInContext(target.externalIdFieldVars, record);
      try {
        String partitionName = target.externalIdFieldEval.eval(target.externalIdFieldVars,
            target.conf.externalIdField, String.class);
        LOG.debug("Expression '{}' is evaluated to '{}' : ", target.conf.externalIdField, partitionName);
        partitions.put(partitionName, sobject);
      } catch (ELEvalException e) {
        LOG.error("Failed to evaluate expression '{}' : ", target.conf.externalIdField, e.toString(), e);
        errorRecords.add(new OnRecordErrorException(record, e.getErrorCode(), e.getParams()));
      }
    }

    for (String externalIdField : partitions.keySet()) {
      Collection<SObject> sobjects = partitions.get(externalIdField);
      SObject[] recordsPerExternalId = sobjects.toArray(new SObject[0]);

      LOG.info("Upserting {} records in Salesforce", recordsPerExternalId.length);

      UpsertResult[] upsertResults = partnerConnection.upsert(externalIdField, recordsPerExternalId);

      LOG.info("{} records upserted in Salesforce", upsertResults.length);

      // check the returned results for any errors
      for (int i = 0; i < upsertResults.length; i++) {
        if (!upsertResults[i].isSuccess()) {
          handleErrorRecord(recordArray[i], upsertResults[i].getErrors(), recordMap, errorRecords);
        }
      }
    }
  }

  private void delete(
      SObject[] recordArray,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ) throws ConnectionException {
    LOG.info("Deleting {} records in Salesforce", recordArray.length);

    DeleteResult[] deleteResults = partnerConnection.delete(sObjectsToIds(recordArray));

    LOG.info("{} records deleted from Salesforce", deleteResults.length);

    // check the returned results for any errors
    for (int i = 0; i < deleteResults.length; i++) {
      if (!deleteResults[i].isSuccess()) {
        handleErrorRecord(recordArray[i], deleteResults[i].getErrors(), recordMap, errorRecords);
      }
    }
  }

  private void undelete(
      SObject[] recordArray,
      Map<SObject, Record> recordMap,
      List<OnRecordErrorException> errorRecords
  ) throws ConnectionException {
    LOG.info("Undeleting {} records in Salesforce", recordArray.length);

    UndeleteResult[] undeleteResults = partnerConnection.undelete(sObjectsToIds(recordArray));

    LOG.info("{} records undeleted in Salesforce", undeleteResults.length);

    // check the returned results for any errors
    for (int i = 0; i < undeleteResults.length; i++) {
      if (!undeleteResults[i].isSuccess()) {
        handleErrorRecord(recordArray[i], undeleteResults[i].getErrors(), recordMap, errorRecords);
      }
    }
  }

  @Override
  public List<OnRecordErrorException> writeBatch(
      String sObjectName,
      Collection<Record> records,
      ForceTarget target
  ) throws StageException {
    Iterator<Record> batchIterator = records.iterator();
    List<OnRecordErrorException> errorRecords = new LinkedList<>();

    // Iterate through entire batch
    while (batchIterator.hasNext()) {
      Map<Integer, List<SObject>> sRecordsByOp = new HashMap<>();
      Map<SObject, Record> recordMap = new HashMap<>();

      // Can only create 200 records per call
      boolean done = false;
      while (batchIterator.hasNext() && !done) {
        Record record = batchIterator.next();
        SObject so = new SObject();
        so.setType(sObjectName);

        int opCode = ForceUtils.getOperationFromRecord(record, target.conf.defaultOperation,
            target.conf.unsupportedAction, errorRecords);
        if (opCode <= 0) {
          continue;
        }
        List<SObject> sRecords = sRecordsByOp.computeIfAbsent(opCode, k -> new ArrayList<>());

        SortedSet<String> columnsPresent = Sets.newTreeSet(fieldMappings.keySet());
        for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
          String sFieldName = mapping.getKey();
          String fieldPath = mapping.getValue();

          // If we're missing fields, skip them.
          if (!record.has(fieldPath)) {
            columnsPresent.remove(sFieldName);
            continue;
          }

          final Object value = record.get(fieldPath).getValue();

          so.setField(sFieldName, value);
        }
        sRecords.add(so);
        recordMap.put(so, record);

        done = sRecords.size() == MAX_RECORDS_CREATE;
      }

      for (Map.Entry<Integer, List<SObject>> entry : sRecordsByOp.entrySet()) {
        List<SObject> sRecords = entry.getValue();
        SObject[] recordArray = sRecords.toArray(new SObject[0]);

        try {
          switch (entry.getKey()) {
            case OperationType.INSERT_CODE:
              create(recordArray, recordMap, errorRecords);
              break;
            case OperationType.DELETE_CODE:
              delete(recordArray, recordMap, errorRecords);
              break;
            case OperationType.UPDATE_CODE:
              update(recordArray, recordMap, errorRecords);
              break;
            case OperationType.UPSERT_CODE:
              upsert(target, recordArray, recordMap, errorRecords);
              break;
            case OperationType.UNDELETE_CODE:
              undelete(recordArray, recordMap, errorRecords);
              break;
          }
        } catch (ConnectionException e) {
          throw new StageException(Errors.FORCE_13,
              ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e)
          );
        }
      }
    }

    return errorRecords;
  }
}
