/*
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
 *      http://www.apache.org/licenses/LICENSE-2.0
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

public class LookupUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LookupUtils.class);

  private LookupUtils() {}

  @NotNull
  @SuppressWarnings("unchecked")
  public static LoadingCache buildCache(CacheLoader store, CacheConfig conf) {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (!conf.enabled) {
      return cacheBuilder.maximumSize(0)
          .build(store);
    }

    if(conf.maxSize == -1) {
      conf.maxSize = Long.MAX_VALUE;
    }

    if(LOG.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if (conf.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.maximumSize(conf.maxSize)
          .expireAfterAccess(conf.expirationTime, conf.timeUnit);
    } else if (conf.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.maximumSize(conf.maxSize)
          .expireAfterWrite(conf.expirationTime, conf.timeUnit);
    } else {
      throw new IllegalArgumentException(
          Utils.format("Unrecognized EvictionPolicyType: '{}'", conf.evictionPolicyType)
      );
    }
    return cacheBuilder.build(store);
  }
}
