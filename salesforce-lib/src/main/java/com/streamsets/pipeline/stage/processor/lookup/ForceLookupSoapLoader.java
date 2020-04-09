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
package com.streamsets.pipeline.stage.processor.lookup;

import com.google.common.cache.CacheLoader;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.salesforce.DataType;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.SoapRecordCreator;
import com.streamsets.pipeline.lib.salesforce.SobjectRecordCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 Retrieve records for the lookup processor via the Salesforce SOAP API
 */
class ForceLookupSoapLoader extends CacheLoader<String, Optional<List<Map<String, Field>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupSoapLoader.class);
  private static final String COUNT = "count";

  private final ForceLookupProcessor processor;

  ForceLookupSoapLoader(ForceLookupProcessor processor) {
    this.processor = processor;
  }

  @Override
  public Optional<List<Map<String, Field>>> load(String key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private Optional<List<Map<String, Field>>> lookupValuesForRecord(String preparedQuery) throws StageException {
    List<Map<String, Field>> lookupItems = new ArrayList<>();
    SobjectRecordCreator recordCreator = (SobjectRecordCreator)processor.getRecordCreator();

    try {
      if (!recordCreator.metadataCacheExists()) {
        recordCreator.buildMetadataCacheFromQuery(processor.partnerConnection, preparedQuery);
      }

      QueryResult queryResult = processor.getConfig().queryAll
          ? processor.partnerConnection.queryAll(preparedQuery)
          : processor.partnerConnection.query(preparedQuery);
      addResult(lookupItems, queryResult);

      while (!queryResult.isDone()) {
        queryResult = processor.partnerConnection.queryMore(queryResult.getQueryLocator());
        addResult(lookupItems, queryResult);
      }

    } catch (ConnectionException e) {
      String message = (e instanceof ApiFault) ? ((ApiFault)e).getExceptionMessage() : e.getMessage();
      LOG.error(Errors.FORCE_17.getMessage(), preparedQuery, message, e);
      throw new OnRecordErrorException(Errors.FORCE_17, preparedQuery, message, e);
    }

    // If no lookup items were found, use defaults
    return lookupItems.isEmpty() ? Optional.empty() : Optional.of(lookupItems);
  }

  private void addResult(List<Map<String, Field>> lookupItems, QueryResult queryResult) throws StageException {
    SObject[] records = queryResult.getRecords();

    LOG.info("Retrieved {} records", records.length);

    if (processor.getRecordCreator().isCountQuery()) {
      // Special case for old-style COUNT() query
      DataType dataType = (processor.columnsToTypes != null)
          ? processor.columnsToTypes.get(COUNT.toLowerCase())
          : DataType.USE_SALESFORCE_TYPE;
      if (dataType == null) {
        dataType = DataType.USE_SALESFORCE_TYPE;
      }
      Field.Type fieldType = (dataType == DataType.USE_SALESFORCE_TYPE)
          ? Field.Type.INTEGER
          : Field.Type.valueOf(dataType.getLabel());
      LinkedHashMap<String, Field> map = new LinkedHashMap<>();
      map.put(COUNT, Field.create(fieldType, queryResult.getSize()));
      lookupItems.add(map);
    } else {
      for (int i = 0; i < records.length; i++) {
        lookupItems.add(((SoapRecordCreator) processor.getRecordCreator()).addFields(
            records[i],
            processor.columnsToTypes));
      }
    }
  }
}
