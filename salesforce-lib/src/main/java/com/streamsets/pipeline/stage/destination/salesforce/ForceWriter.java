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

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for a record writer implementation that writes records to a
 * Salesforce destination. Implementations may use different wire protocols
 * to write records to Salesforce.
 */
public abstract class ForceWriter {
  final Map<String, String> fieldMappings;
  final String sObject;
  final PartnerConnection partnerConnection;

  /**
   * Constructs and sets field mappings for the writer.
   * @param customMappings Mappings of SDC field names to Salesforce field names
   */
  public ForceWriter(PartnerConnection partnerConnection, String sObject, Map<String, String> customMappings) throws
      ConnectionException {
    this.partnerConnection = partnerConnection;
    this.sObject = sObject;

    // Get default field mappings and add them to the configured mappings
    this.fieldMappings = new HashMap<String, String>(customMappings);
    DescribeSObjectResult result = partnerConnection.describeSObject(sObject);
    for (Field field : result.getFields()) {
      String fieldName = field.getName();
      if (!customMappings.containsKey(fieldName)) {
        fieldMappings.put(fieldName, "/" + fieldName);
      }
    }
  }

  /**
   * Write a batch of records to Salesforce, mapping field names as specified
   * when the writer was created.
   *
   * @param sObjectName Salesforce object name, e.g. 'Account' or 'Transaction__c
   * @param records Collection of records to write
   * @return
   * @throws StageException if an error occurred writing the records
   */
  abstract List<OnRecordErrorException> writeBatch(
      String sObjectName,
      Collection<Record> records,
      ForceTarget target
  ) throws StageException;
}
