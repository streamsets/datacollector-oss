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
package com.streamsets.pipeline.stage.processor.kv.local;

import com.google.common.base.Optional;
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
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.kv.Errors;
import com.streamsets.pipeline.stage.processor.kv.LookupParameterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.stage.processor.kv.LookupMode.BATCH;
import static com.streamsets.pipeline.stage.processor.kv.LookupMode.RECORD;

public class LocalLookupProcessor extends BaseProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(LocalLookupProcessor.class);

  private final LocalLookupConfig conf;

  private ErrorRecordHandler error;
  private ELEval keyExprEval;

  protected LocalStore store;

  public LocalLookupProcessor(LocalLookupConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if(this.conf.values.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.LOCAL.name(),
          this.conf.values.toString(),
          Errors.LOOKUP_03
      ));
    }

    if (issues.isEmpty()) {
      error = new DefaultErrorRecordHandler(getContext());
      keyExprEval = getContext().createELEval("keyExpr");
      store = conf.createStore();
    }

    return issues;
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
        Optional<String> value = store.get(key);
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
      Map<String, Optional<String>> values = store.get(keys);

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
    } catch (Exception e) {
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
    Set<String> keys = new HashSet<>();
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


