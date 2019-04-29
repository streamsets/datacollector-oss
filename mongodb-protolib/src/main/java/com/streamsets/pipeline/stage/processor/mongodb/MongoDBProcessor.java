/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.processor.mongodb;

import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;

import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;

public class MongoDBProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBProcessor.class);

  private final MongoDBProcessorConfigBean configBean;
  private ErrorRecordHandler errorRecordHandler;
  private MongoClient mongoClient;
  private MongoCollection<Document> mongoCollection;
  private LoadingCache<Document, Optional<List<Map<String, Field>>>> cache;
  private CacheCleaner cacheCleaner;

  protected MongoDBProcessor(MongoDBProcessorConfigBean config) {
    configBean= config;
  }

  @Override
  protected List<ConfigIssue> init() {
      final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    configBean.mongoConfig.init(getContext(), issues, configBean.readPreference.getReadPreference(), null);
    if (!issues.isEmpty()){
      return issues;
    }

    // since no issue was found in validation, the followings must not be null at this point.
    Utils.checkNotNull(configBean.mongoConfig.getMongoDatabase(), "MongoDatabase");
    mongoClient = Utils.checkNotNull(configBean.mongoConfig.getMongoClient(), "MongoClient");
    mongoCollection = Utils.checkNotNull(configBean.mongoConfig.getMongoCollection(), "MongoCollection");

    if (configBean.fieldMapping.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.MONGODB.name(),
          "configBean.fieldMapping",
          Errors.MONGODB_41,
          Errors.MONGODB_41.getMessage()
      ));
    }
    if (!issues.isEmpty()){
      return issues;
    }

    MongoDBLookupLoader lookupLoader = new MongoDBLookupLoader(mongoCollection);
    cache = LookupUtils.buildCache(lookupLoader, configBean.cacheConfig);
    cacheCleaner = new CacheCleaner(cache, "MongoDBProcessor", 10 * 60 * 1000);
    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    // MongoDB's Java driver doesn't support bulk lookup. So perform lookup per record
    super.process(batch, batchMaker);
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // Construct a document for lookup filter
    Document query = new Document();
    for (MongoDBFieldColumnMapping mapping: configBean.fieldMapping) {
      // if record doesn't have a field specified in the mapping, or value is null,
      // exclude the field from filter, instead of sending to error.
      if (record.has(mapping.sdcField) && record.get(mapping.sdcField) != null) {
        query.append(mapping.keyName, record.get(mapping.sdcField).getValue());
      }
    }
    // If all of the filters are missing in record, we cannot perform lookup.
    if (query.isEmpty()) {
      throw new OnRecordErrorException(Errors.MONGODB_42, record);
    }

    Optional<List<Map<String, Field>>> entry;
    try {
      entry = cache.get(query);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), StageException.class);
      throw new IllegalStateException(e); // The cache loader shouldn't throw anything that isn't a StageException.
    }

    if (entry.isPresent()) {
      List<Map<String, Field>> values = entry.get();
      switch (configBean.multipleValuesBehavior) {
        case FIRST_ONLY:
          setFieldsInRecord(record, values.get(0));
          batchMaker.addRecord(record);
          break;
        case SPLIT_INTO_MULTIPLE_RECORDS:
          for (Map<String, Field> lookupItem : values) {
            Record newRecord = getContext().cloneRecord(record);
            setFieldsInRecord(newRecord, lookupItem);
            batchMaker.addRecord(newRecord);
          }
          break;
        default:
          throw new IllegalStateException("Unknown multiple value behavior: " + configBean.multipleValuesBehavior);
      }
    } else {
      // No results
      switch (configBean.missingValuesBehavior) {
        case SEND_TO_ERROR:
          LOG.error(Errors.MONGODB_40.getMessage(), query.toJson());
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.MONGODB_40, query.toJson()));
          break;
        case PASS_RECORD_ON:
          batchMaker.addRecord(record);
          break;
        default:
          throw new IllegalStateException("Unknown missing value behavior: " + configBean.missingValuesBehavior);
      }
    }
  }

  /**
   * Set the lookup reuslt in the result field
   * @param record Lookup result
   * @param fields Field name configured in "result field"
   */
  private void setFieldsInRecord(Record record, Map<String, Field> fields) {
    record.set(configBean.resultField, Field.createListMap(new LinkedHashMap<>(fields)));
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(mongoClient);
    super.destroy();
  }
}
