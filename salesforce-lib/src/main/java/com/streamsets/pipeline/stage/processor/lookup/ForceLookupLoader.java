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

import java.util.Map;

class ForceLookupLoader extends CacheLoader<String, Map<String, Field>> {
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupLoader.class);

  private final ForceLookupProcessor processor;

  ForceLookupLoader(ForceLookupProcessor processor) {
    this.processor = processor;
  }

  @Override
  public Map<String, Field> load(String key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private Map<String, Field> lookupValuesForRecord(String preparedQuery) throws StageException {
    try {
      if (!processor.recordCreator.metadataCacheExists()) {
        processor.recordCreator.buildMetadataCacheFromQuery(processor.partnerConnection, preparedQuery);
      }

      QueryResult queryResult = processor.conf.queryAll
          ? processor.partnerConnection.queryAll(preparedQuery)
          : processor.partnerConnection.query(preparedQuery);

      SObject[] records = queryResult.getRecords();

      LOG.info("Retrieved {} records", records.length);

      if (records.length > 0) {
        // TODO - handle multiple records (SDC-4739)

        return processor.recordCreator.addFields(
            records[0],
            processor.columnsToTypes);
      } else {
        // Salesforce returns no row. Use default values.
        return processor.getDefaultFields();
      }
    } catch (ConnectionException e) {
      LOG.error(Errors.FORCE_17.getMessage(), preparedQuery, e);
      throw new OnRecordErrorException(Errors.FORCE_17, preparedQuery, e.getMessage());
    }
  }
}
