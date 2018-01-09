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
package com.streamsets.pipeline.stage.processor.kv;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.impl.Utils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class LookupUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LookupUtils.class);

  private LookupUtils() {}

  @NotNull
  @SuppressWarnings("unchecked")
  public static<Key, Value> LoadingCache<Key, Value> buildCache(CacheLoader<Key, Value> cacheLoader, CacheConfig conf) {
    if(conf.retryOnCacheMiss) {
      throw new IllegalArgumentException("This stage does not support retry on cache miss feature.");
    }

    return createBuilder(conf).build(cacheLoader);
  }

  @NotNull
  @SuppressWarnings("unchecked")
  public static<Key, Value> LoadingCache<Key, Optional<Value>> buildCache(
    CacheLoader<Key, Optional<Value>> cacheLoader,
    CacheConfig conf,
    Optional<Value> defaultValue
  ) {
    return new OptionalLoadingCache(
      !conf.retryOnCacheMiss,
      createBuilder(conf).build(cacheLoader),
      defaultValue
    );
  }

  private static CacheBuilder createBuilder(CacheConfig conf) {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();

    if(!conf.enabled) {
      // If cache is disabled, simply set size to zero
      conf.maxSize = 0;
    } else if(conf.maxSize == -1) {
      conf.maxSize = Long.MAX_VALUE;
    }

    // recordStats is available only in Guava 12.0 and above, but
    // CDH still uses guava 11.0. Hence the reflection.
    if(LOG.isDebugEnabled()) {
      try {
        Method m = CacheBuilder.class.getMethod("recordStats");
        if (m != null) {
          m.invoke(cacheBuilder);
        }
      } catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException e) {
        // We're intentionally ignoring any reflection errors as we might be running
        // with old guava on class path.
      }
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if (conf.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder
          .maximumSize(conf.maxSize)
          .expireAfterAccess(conf.expirationTime, conf.timeUnit)
      ;
    } else if (conf.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
      cacheBuilder
          .maximumSize(conf.maxSize)
          .expireAfterWrite(conf.expirationTime, conf.timeUnit)
      ;
    } else {
      throw new IllegalArgumentException(
          Utils.format("Unrecognized EvictionPolicyType: '{}'", conf.evictionPolicyType)
      );
    }

    return cacheBuilder;
  }
}
