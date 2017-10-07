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
package com.streamsets.pipeline.stage.processor.kv.redis;

import com.google.common.base.Optional;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
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
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.redis.DataType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.net.URI;
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

public class RedisLookupProcessor extends BaseProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(RedisLookupProcessor.class);

  private final RedisLookupConfig conf;

  private ErrorRecordHandler error;
  private ELEval keyExprEval;
  private LoadingCache<Pair<String, DataType>, LookupValue> cache;

  private RedisStore store;
  private CacheCleaner cacheCleaner;

  public RedisLookupProcessor(RedisLookupConfig conf) {
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    JedisPool pool = null;

    try {
      pool = new JedisPool(poolConfig, URI.create(conf.uri), conf.connectionTimeout * 1000); // connectionTimeout value is in seconds
      Jedis jedis = pool.getResource();
      jedis.ping();
      jedis.close();
    } catch (JedisException e) { // NOSONAR
      issues.add(getContext().createConfigIssue("REDIS", "conf.uri", Errors.REDIS_LOOKUP_01, conf.uri, e.toString()));
    } catch (IllegalArgumentException e) {
      issues.add(getContext().createConfigIssue("REDIS", "conf.uri", Errors.REDIS_LOOKUP_02, conf.uri, e.toString()));
    } finally {
      try {
        if (pool != null) {
          pool.close();
        }
      } catch (JedisException ignored) { // NOSONAR
      }
    }

    if (issues.isEmpty()) {
      error = new DefaultErrorRecordHandler(getContext());
      keyExprEval = getContext().createELEval("keyExpr");
      store = new RedisStore(conf);
      cache = LookupUtils.buildCache(store, conf.cache);

      cacheCleaner = new CacheCleaner(cache, "RedisLookupProcessor", 10 * 60 * 1000);
    }

    return issues;
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
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
    for (RedisLookupParameterConfig parameters : conf.lookups) {
      try {
        final String key = keyExprEval.eval(elVars, parameters.keyExpr, String.class);
        final Pair<String, DataType> pair = Pair.of(key, parameters.dataType);
        LookupValue value = cache.getUnchecked(pair);
        updateRecord(value, parameters.outputFieldPath, record);
      } catch (ELEvalException e) {
        LOG.error(Errors.LOOKUP_01.getMessage(), parameters.keyExpr, e);
        error.onError(
            new OnRecordErrorException(
                record,
                Errors.LOOKUP_01,
                parameters.keyExpr
            )
        );
      } catch (UncheckedExecutionException e) {
        LOG.error(e.getMessage(), keyExprEval.eval(elVars, parameters.keyExpr, String.class),
            parameters.dataType, e );
        error.onError(
            new OnRecordErrorException(
                record,
                Errors.REDIS_LOOKUP_03,
                e.getMessage(),
                keyExprEval.eval(elVars, parameters.keyExpr, String.class),
                parameters.dataType
            )
        );
      }
    }
  }

  private void doBatchLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records;
    try {
      List<Map<String, Pair<String, DataType>>> mapList = getMap(batch);
      Set<Pair<String, DataType>> keys = getKeys(mapList);
      Map<Pair<String, DataType>, LookupValue> values = cache.getAll(keys);

      records = batch.getRecords();
      Record record;
      int index = 0;
      Map<String, Pair<String, DataType>> map;

      while (records.hasNext()) {
        record = records.next();
        map = mapList.get(index);
        // Now we have to get the key for each key configuration
        for (RedisLookupParameterConfig parameters : conf.lookups) {
          Pair<String, DataType> key = map.get(parameters.outputFieldPath);
          LookupValue value = values.get(key);
          updateRecord(value, parameters.outputFieldPath, record);
        }

        batchMaker.addRecord(record);
        index++;
      }

    } catch (ELEvalException e) {
      LOG.error("Failed to fetch values from cache: {}", e.toString(), e);
      // Send whole batch to error
      records = batch.getRecords();
      while (records.hasNext()) {
        Record record = records.next();
        error.onError(new OnRecordErrorException(record, Errors.LOOKUP_01, e.toString()));
      }
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch values from cache: {}", e.toString(), e);
      // Send whole batch to error
      records = batch.getRecords();
      while (records.hasNext()) {
        Record record = records.next();
        error.onError(new OnRecordErrorException(record, Errors.LOOKUP_02, e.toString()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void updateRecord(LookupValue value, String outputFieldPath, Record record) throws StageException {
    switch(value.getType()) {
      case STRING:
        Optional<String> string = Optional.fromNullable((String)value.getValue());
        if(Optional.fromNullable(value.getValue()).isPresent()) {
          record.set(outputFieldPath, Field.create(string.get()));
        }
        break;
      case LIST:
        List<Field> field = new ArrayList<>();
        List<String> list = (List<String>) value.getValue();
        if (!list.isEmpty()) {
          for (String element : list) {
            field.add(Field.create(element));
          }
          record.set(outputFieldPath, Field.create(field));
        }
        break;
      case HASH:
        Map<String, Field> fieldMap = new HashMap<>();
        HashMap<String, String> map = (HashMap<String, String>) value.getValue();
        if (!map.isEmpty()) {
          for (Map.Entry<String, String> entry : map.entrySet()) {
            fieldMap.put(entry.getKey(), Field.create(entry.getValue()));
          }
          record.set(outputFieldPath, Field.create(fieldMap));
        }
        break;
      case SET:
        field = new ArrayList<>();
        Set<String> set = (Set<String>) value.getValue();
        if (!set.isEmpty()) {
          for (String element : set) {
            field.add(Field.create(element));
          }
          record.set(outputFieldPath, Field.create(field));
        }
        break;
      default:
        LOG.error(
            Errors.REDIS_LOOKUP_04.getMessage(),
            value.getType().getLabel(),
            record.getHeader().getSourceId()
        );
        throw new StageException(
            Errors.REDIS_LOOKUP_04,
            value.getType().getLabel(),
            record.getHeader().getSourceId()
        );
    }
  }

  private List<Map<String, Pair<String, DataType>>> getMap(Batch batch) throws ELEvalException {
    List<Map<String, Pair<String, DataType>>> list = new ArrayList<>();
    Iterator<Record> records = batch.getRecords();

    while (records.hasNext()) {
      Record record = records.next();
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      Map<String, Pair<String, DataType>> keys = new HashMap<>();

      for (RedisLookupParameterConfig parameters : conf.lookups) {
        Pair<String, DataType> pair = Pair.of(keyExprEval.eval(elVars, parameters.keyExpr, String.class), parameters.dataType);
        keys.put(parameters.outputFieldPath, pair);
      }
      list.add(keys);
    }

    return list;
  }

  private Set<Pair<String, DataType>> getKeys(List<Map<String, Pair<String, DataType>>> mapList) {
    Set<Pair<String, DataType>> keys = new HashSet<>();

    for(Map<String, Pair<String, DataType>> map : mapList) {
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
