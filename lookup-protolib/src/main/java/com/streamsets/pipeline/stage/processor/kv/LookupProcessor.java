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
package com.streamsets.pipeline.stage.processor.kv;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.processor.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.lib.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.streamsets.pipeline.stage.processor.kv.LookupMode.BATCH;
import static com.streamsets.pipeline.stage.processor.kv.LookupMode.RECORD;

public class LookupProcessor extends BaseProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(LookupProcessor.class);

  private final LookupProcessorConfig conf;

  private ErrorRecordHandler error;
  private ELEval keyExprEval;
  private LoadingCache<String, Optional<String>> cache;

  protected Store store;

  public LookupProcessor(LookupProcessorConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.init(getContext(), issues);

    if (issues.isEmpty()) {
      error = new DefaultErrorRecordHandler(getContext());
      keyExprEval = getContext().createELEval("keyExpr");
      store = conf.createStore();

      cache = buildCache();
    }

    return issues;
  }

  private LoadingCache<String, Optional<String>> buildCache() {
    CacheBuilder build = CacheBuilder.newBuilder();
    if (!conf.cache.enabled) {
      return build.maximumSize(0)
          .build(new StoreCacheLoader(store));
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if(conf.cache.maxSize == -1) {
      conf.cache.maxSize = Long.MAX_VALUE;
    }
    build.maximumSize(conf.cache.maxSize);
    if (conf.cache.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
          build.expireAfterAccess(conf.cache.expirationTime, conf.cache.timeUnit);
    } else if (conf.cache.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
          build.expireAfterWrite(conf.cache.expirationTime, conf.cache.timeUnit);
    } else {
      throw new IllegalArgumentException(
          Utils.format("Unrecognized EvictionPolicyType: '{}'", conf.cache.evictionPolicyType)
      );
    }
    return build.build(new StoreCacheLoader(store));
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // empty batch
      return;
    }
    if (conf.mode == BATCH) {
      doBatchLookup(batch, batchMaker);
    } else if (conf.mode == RECORD) {
      doRecordLookup(batch, batchMaker);
    } else {
      throw new IllegalArgumentException(Utils.format("Unrecognized lookup mode: '{}'", conf.mode));
    }
  }

  private void doRecordLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      lookupValuesForRecord(record);
      batchMaker.addRecord(record);
    }
  }

  private void lookupValuesForRecord(Record record) throws StageException {
    ELVars elVars = getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);
    // Now we have to get the key for each key configuration
    for (LookupParameterConfig parameters : conf.lookups) {
      try {
        final String key = keyExprEval.eval(elVars, parameters.keyExpr, String.class);
        Optional<String> value = cache.getUnchecked(key);
        if (value.isPresent()) {
          record.set(parameters.outputFieldPath, Field.create(value.get()));
        }
      } catch (ELEvalException e) {
        LOG.error(Errors.LOOKUP_01.getMessage(), parameters.keyExpr, e);
        error.onError(new OnRecordErrorException(Errors.LOOKUP_01, parameters.keyExpr, record));
      }
    }
  }

  private void doBatchLookup(Batch batch, BatchMaker batchMaker) throws StageException{
    // List of <outputField, key> per records
    List<Map<String, String>> mapList = getMap(batch);
    // Get set of keys
    Set<String> keys = getKeys(mapList);

    Iterator<Record> records;

    try {
      Map<String, Optional<String>> values = cache.getAll(keys);

      records = batch.getRecords();
      Record record = null;
      int index = 0;
      Map<String, String> map = null;

      while (records.hasNext()) {
        record = records.next();
        map = mapList.get(index);
        // Now we have to get the key for each key configuration
        for (LookupParameterConfig parameters : conf.lookups) {
          String key = map.get(parameters.outputFieldPath);
          Optional<String> value = values.get(key);
          if (value.isPresent()) {
            record.set(parameters.outputFieldPath, Field.create(value.get()));
          }
        }

        batchMaker.addRecord(record);
        index++;
      }
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch values from cache: {}", e.toString(), e);
      // Send whole batch to error
      records = batch.getRecords();
      while (records.hasNext()) {
        Record record = records.next();
        error.onError(new OnRecordErrorException(record, Errors.LOOKUP_02, e.toString()));
      }
      return;
    }
  }

  private List<Map<String, String>> getMap(Batch batch) throws StageException {
    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    Iterator<Record> records = batch.getRecords();

    while (records.hasNext()) {
      Record record = records.next();
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      Map<String, String> keys = new HashMap<String, String>();

      for (LookupParameterConfig parameters : conf.lookups) {
        try {
          keys.put(parameters.outputFieldPath, keyExprEval.eval(elVars, parameters.keyExpr, String.class));
        } catch (ELEvalException e) {
          LOG.error(Errors.LOOKUP_01.getMessage(), parameters.keyExpr, e);
          error.onError(new OnRecordErrorException(Errors.LOOKUP_01, parameters.keyExpr, record));
        }
      }
      list.add(keys);
    }

    return list;
  }

  private Set<String> getKeys(List<Map<String, String>> mapList) {
    Set<String> keys = new HashSet<String>();
    for(Map<String, String> map : mapList) {
      keys.addAll(map.values());
    }
    return keys;
  }

  @Override
  public void destroy() {
    super.destroy();
    if (store != null) {
      try {
        store.close();
      } catch (Exception e) {
        LOG.error(e.toString(), e);
      }
    }
  }
}
