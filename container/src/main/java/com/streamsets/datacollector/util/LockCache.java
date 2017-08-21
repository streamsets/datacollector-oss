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
package com.streamsets.datacollector.util;

import javax.inject.Inject;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Cache providing mapping of keys to values. Entries stay in cache till
 * they are evicted.
 * @param <K>
 */
public class LockCache<K> {
  private LoadingCache<K, Object> cache;

  @Inject
  public LockCache() {
    cache = CacheBuilder.newBuilder().weakValues().build(new CacheLoader<K, Object>() {
      @Override
      public Object load(K key) {
        return new Object();
      }
    });
  }

  /**
   * Returns value associated with resource in cache if exists, first loading that value if necessary.
   * @param resource the key in cache
   * @return
   */
  public Object getLock(K resource) {
    return cache.getUnchecked(resource);
  }

}


