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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface for a record writer implementation that writes records to a
 * Salesforce destination. Implementations may use different wire protocols
 * to write records to Salesforce.
 */
public abstract class ForceWriter {
  final Map<String, String> fieldMappings;

  /**
   * Constructs and sets field mappings for the writer.
   * @param fieldMappings Mappings of SDC field names to Salesforce field names
   */
  public ForceWriter(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
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
