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
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StoreCacheLoader extends CacheLoader<String, Optional<String>> {
  private final Store store;

  public StoreCacheLoader(Store store) {
    this.store = store;
  }

  @Override
  public Optional<String> load(String key) throws Exception {
    return store.get(key);
  }

  @Override
  public Map<String, Optional<String>> loadAll(Iterable<? extends String> keys) throws Exception {
    List<String> keyList = Lists.newArrayList(keys);
    List<Optional<String>> values = store.get(keyList);

    Iterator<? extends String> keyIterator = keys.iterator();
    Iterator<Optional<String>> valueIterator = values.iterator();

    Map<String, Optional<String>> result = new HashMap<>(keyList.size());
    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      result.put(keyIterator.next(), valueIterator.next());
    }
    return result;
  }
}
