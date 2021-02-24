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
package com.streamsets.pipeline.stage.processor.schemagen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;
import com.streamsets.pipeline.stage.processor.schemagen.generators.SchemaGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SchemaGeneratorProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaGeneratorProcessor.class);

  @VisibleForTesting
  static final String CACHE_KEY = "stageRunnerCacheKey";

  private final SchemaGeneratorConfig config;
  private SchemaGenerator generator;

  /**
   * Optional cache for schemas.
   */
  private Cache<String, String> cache;
  private ELVars cacheKeyExpressionVars;
  private ELEval cacheKeyExpressionEval;
  private Map<String, Object> gaugeMap;
  private static final String CACHE_HIT_RATE = "Hit Rate";
  private static final String CACHE_MISS_RATE = "Miss Rate";
  private static final String CACHE_LOAD_EXCEPTION_RATE = "Load Exception Rate";
  private static final String CACHE_COUNT = "Loaded Entries";

  public SchemaGeneratorProcessor(SchemaGeneratorConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // Instantiate configured generator
    try {
      this.generator = config.schemaType.getGenerator().newInstance();
      issues.addAll(generator.init(config, getContext()));
    } catch (InstantiationException|IllegalAccessException e) {
      issues.add(getContext().createConfigIssue("SCHEMA", "config.schemaType", Errors.SCHEMA_GEN_0001, e.toString()));
    }

    // Instantiate cache if needed
    if(config.enableCache) {
      this.gaugeMap = getContext().createGauge("Cache").getValue();
      this.gaugeMap.put(CACHE_HIT_RATE, 0);
      this.gaugeMap.put(CACHE_MISS_RATE, 0);
      this.gaugeMap.put(CACHE_LOAD_EXCEPTION_RATE, 0);
      this.gaugeMap.put(CACHE_COUNT, 0);

      Map<String, Object> runnerSharedMap = getContext().getStageRunnerSharedMap();
      synchronized (runnerSharedMap) {
        cache = (Cache<String, String>) runnerSharedMap.computeIfAbsent(CACHE_KEY,
          key -> CacheBuilder.newBuilder()
            .maximumSize(config.cacheSize)
            .recordStats()
            .build()
        );
      }

      cacheKeyExpressionEval = getContext().createELEval("cacheKeyExpression");
      cacheKeyExpressionVars = getContext().createELVars();
    }

    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) {
    // We delegate up normal execution, just calculate stats at the end of the batch
    super.process(batch, batchMaker);

    // Update metrics
    if(config.enableCache) {
      CacheStats stats = cache.stats();

      this.gaugeMap.put(CACHE_HIT_RATE, stats.hitRate());
      this.gaugeMap.put(CACHE_MISS_RATE, stats.missRate());
      this.gaugeMap.put(CACHE_LOAD_EXCEPTION_RATE, stats.loadExceptionRate());
      this.gaugeMap.put(CACHE_COUNT, cache.size());
    }
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    String schema = null;

    // If caching is enabled, try to resolve cache first
    if(config.enableCache) {
      RecordEL.setRecordInContext(cacheKeyExpressionVars, record);
      String cacheKey = cacheKeyExpressionEval.eval(cacheKeyExpressionVars, config.cacheKeyExpression, String.class);

      try {
        // Get schema from the cache
        schema = cache.get(cacheKey, () -> generator.generateSchema(record));
      } catch (ExecutionException e) {
        LOG.error("Execution exception while creating schema", e);
        Throwables.throwIfInstanceOf(e.getCause(), OnRecordErrorException.class);
        Throwables.throwIfInstanceOf(e.getCause(), StageException.class);
        throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0010, record.getHeader().getSourceId(), e.toString());
      }
    } else {
      // Otherwise simply calculate the schema each time
      schema = generator.generateSchema(record);
    }

    record.getHeader().setAttribute(config.attributeName, schema);
    batchMaker.addRecord(record);
  }

}
