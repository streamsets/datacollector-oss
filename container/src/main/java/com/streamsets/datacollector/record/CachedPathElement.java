/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.record;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.List;
import java.util.function.Supplier;

/**
 * Parsing Path into PathElement is fairly heavy operation due to various regexps replacements that we need to perform
 * for backward compatibility. As it's likely that a single thread (processing the same pipeline) will access the same
 * field paths repeatedly we can cache the various fields paths and their equivalent PathElements.
 */
public class CachedPathElement {

  private static ThreadLocal<LoadingCache<String, List<PathElement>>> threadLocalCache = ThreadLocal.withInitial(new Supplier<LoadingCache<String, List<PathElement>>>() {
    @Override
    public LoadingCache<String, List<PathElement>> get() {
      return CacheBuilder.newBuilder()
        .maximumSize(1000) // Currently hard-coded value
        .build(new CacheLoader<String, List<PathElement>>() {
          @Override
          public List<PathElement> load(String key) throws Exception {
            return PathElement.parse(key, true);
          }
        });
    }
  });

  public static List<PathElement> parse(String fieldPath) {
    return threadLocalCache.get().getUnchecked(fieldPath);
  }
}
