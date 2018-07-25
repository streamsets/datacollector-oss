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
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.salesforce.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ForceLookupLoader extends CacheLoader<String, Optional<List<Map<String, Field>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupLoader.class);

  private final ForceLookupProcessor processor;

  ForceLookupLoader(ForceLookupProcessor processor) {
    this.processor = processor;
  }

  @Override
  public Optional<List<Map<String, Field>>> load(String key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private Optional<List<Map<String, Field>>> lookupValuesForRecord(String preparedQuery) throws StageException {
    List<Map<String, Field>> lookupItems = new ArrayList<>();

    try {
      if (!processor.recordCreator.metadataCacheExists()) {
        processor.recordCreator.buildMetadataCacheFromQuery(processor.partnerConnection, preparedQuery);
      }

      QueryResult queryResult = processor.conf.queryAll
          ? processor.partnerConnection.queryAll(preparedQuery)
          : processor.partnerConnection.query(preparedQuery);

      SObject[] records = queryResult.getRecords();

      LOG.info("Retrieved {} records", records.length);

      for (int i = 0; i < records.length; i++) {
        lookupItems.add(processor.recordCreator.addFields(
            records[i],
            processor.columnsToTypes));
      }

      // If no lookup items were found, use defaults
      if(lookupItems.isEmpty()) {
        return Optional.empty();
      }
    } catch (ConnectionException e) {
      LOG.error(Errors.FORCE_17.getMessage(), preparedQuery, e);
      throw new OnRecordErrorException(Errors.FORCE_17, preparedQuery, e.getMessage());
    }

    return Optional.of(lookupItems);
  }
}
