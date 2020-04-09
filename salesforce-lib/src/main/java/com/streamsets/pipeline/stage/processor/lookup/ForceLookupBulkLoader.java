/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.salesforce.ForceBulkReader;
import com.streamsets.pipeline.lib.salesforce.ForceCollector;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 Retrieve records for the lookup processor via the Salesforce Bulk API
 */
class ForceLookupBulkLoader extends CacheLoader<String, Optional<List<Map<String, Field>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(ForceLookupBulkLoader.class);
  private static final int BATCH_SIZE = 1000;

  private final ForceBulkReader bulkReader;
  private final ForceLookupProcessor processor;

  ForceLookupBulkLoader(ForceLookupProcessor processor) {
    this.processor = processor;
    this.bulkReader = new ForceBulkReader(processor);
  }

  @Override
  public Optional<List<Map<String, Field>>> load(String key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private Optional<List<Map<String, Field>>> lookupValuesForRecord(String preparedQuery) throws StageException {
    List<Map<String, Field>> lookupItems = new ArrayList<>();

    String offset = "";
    while (offset != null) {
      offset = bulkReader.produce(offset, BATCH_SIZE, new ForceCollector() {
        @Override
        public String addRecord(LinkedHashMap<String, Field> f, int numRecords) {
          lookupItems.add(f);
          return "";
        }

        @Override
        public String prepareQuery(String offset) {
          return preparedQuery;
        }

        @Override
        public boolean isDestroyed() {
          return processor.isDestroyed();
        }
      });
    }

    return lookupItems.isEmpty() ? Optional.empty() : Optional.of(lookupItems);
  }
}
