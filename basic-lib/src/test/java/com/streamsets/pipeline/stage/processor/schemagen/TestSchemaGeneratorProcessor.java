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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaGeneratorProcessor {

  @Test
  public void testCacheNewEntries() throws StageException {
    SchemaGeneratorConfig config = new SchemaGeneratorConfig();
    config.enableCache = true;
    config.cacheKeyExpression = "key"; // Constant

    ProcessorRunner runner = new ProcessorRunner.Builder(SchemaGeneratorDProcessor.class, new SchemaGeneratorProcessor(config))
      .addOutputLane("a")
      .build();
    runner.runInit();

    // Cache is empty at the begging
    Cache<String, String> cache = (Cache<String, String>) runner.getContext().getStageRunnerSharedMap().get(SchemaGeneratorProcessor.CACHE_KEY);
    Assert.assertNotNull(cache);
    Assert.assertNull(cache.getIfPresent("key"));

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "a", Field.create(Field.Type.STRING, "Arvind")
    )));

    runner.runProcess(ImmutableList.of(record));

    // Now a new key should exist for this record in teh cache
    Assert.assertNotNull(cache.getIfPresent("key"));
  }

  @Test
  public void testCacheUseExistingEntry() throws StageException {
    SchemaGeneratorConfig config = new SchemaGeneratorConfig();
    config.enableCache = true;
    config.cacheKeyExpression = "key"; // Constant

    ProcessorRunner runner = new ProcessorRunner.Builder(SchemaGeneratorDProcessor.class, new SchemaGeneratorProcessor(config))
      .addOutputLane("a")
      .build();
    runner.runInit();

    // Cache is empty at the begging
    Cache<String, String> cache = (Cache<String, String>) runner.getContext().getStageRunnerSharedMap().get(SchemaGeneratorProcessor.CACHE_KEY);
    cache.put("key", "fake-schema");

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.LIST_MAP, ImmutableMap.of(
      "a", Field.create(Field.Type.STRING, "Arvind")
    )));

    Record outputRecord = runner.runProcess(ImmutableList.of(record)).getRecords().get("a").get(0);

    Assert.assertEquals("fake-schema", outputRecord.getHeader().getAttribute("avroSchema"));

  }

}
