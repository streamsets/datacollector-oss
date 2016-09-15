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
package com.streamsets.pipeline.stage.destination.salesforce;

import com.google.common.collect.Sets;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
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

  @Override
  public List<OnRecordErrorException> writeBatch(
      String sObjectName,
      Collection<Record> records
  ) throws StageException {
    Iterator<Record> batchIterator = records.iterator();
    List<OnRecordErrorException> errorRecords = new LinkedList<>();

    while (batchIterator.hasNext()) {
      List<SObject> sRecords = new ArrayList<>();
      Map<SObject, Record> recordMap = new HashMap<>();

      for (int recordCount = 0; recordCount < MAX_RECORDS_CREATE && batchIterator.hasNext(); recordCount++) {
        Record record = batchIterator.next();
        SObject so = new SObject();

        so.setType(sObjectName);

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
      }

      SObject[] recordArray = sRecords.toArray(new SObject[0]);

      LOG.info("Writing {} records to Salesforce", recordArray.length);

      // create the records in Salesforce.com
      SaveResult[] saveResults = new SaveResult[0];
      try {
        saveResults = partnerConnection.create(recordArray);
      } catch (ConnectionException e) {
        throw new StageException(Errors.FORCE_13, ForceUtils.getExceptionCode(e) + ", " + ForceUtils.getExceptionMessage(e));
      }

      LOG.info("{} records written to Salesforce", saveResults.length);

      // check the returned results for any errors
      for (int i=0; i< saveResults.length; i++) {
        if (!saveResults[i].isSuccess()) {
          Record record = recordMap.get(recordArray[i]);
          String errorString = "";
          for (Error error : saveResults[i].getErrors()) {
            if (errorString.length() > 0) {
              errorString += "\n";
            }
            errorString += error.getMessage();
          }
          errorRecords.add(new OnRecordErrorException(
              record,
              Errors.FORCE_11,
              record.getHeader().getSourceId(),
              errorString
          ));
        }
      }
    }

    return errorRecords;
  }
}
