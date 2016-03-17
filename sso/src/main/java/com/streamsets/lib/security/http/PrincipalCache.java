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
package com.streamsets.lib.security.http;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class PrincipalCache {
  private static final Object DUMMY = new Object();

  private final Cache<String, SSOUserPrincipal> principalsCache;
  private final Cache<String, Object> invalidatedTokens;


  public PrincipalCache() {
    this(TimeUnit.MINUTES.toMillis(30), TimeUnit.HOURS.toMillis(24));
  }

  public PrincipalCache(long principalCacheExpirationMillis, long invalidatedTokensCacheExpirationMillis) {
    principalsCache =
        CacheBuilder.newBuilder().expireAfterWrite(principalCacheExpirationMillis, TimeUnit.MILLISECONDS).build();
    invalidatedTokens = CacheBuilder
        .newBuilder()
        .expireAfterWrite(invalidatedTokensCacheExpirationMillis, TimeUnit.MILLISECONDS)
        .build();
  }

  public SSOUserPrincipal get(String autToken) {
    return principalsCache.getIfPresent(autToken);
  }

  public boolean isInvalid(String authToken) {
    return invalidatedTokens.getIfPresent(authToken) != null;
  }

  public synchronized boolean put(String authToken, SSOUserPrincipal principal) {
    boolean cached = false;
    if (invalidatedTokens.getIfPresent(authToken) == null && principalsCache.getIfPresent(authToken) == null) {
      principalsCache.put(authToken, principal);
      cached = true;
    }
    return cached;
  }

  public synchronized boolean invalidate(String authToken) {
    boolean invalidate = false;
    if (!isInvalid(authToken)) {
      principalsCache.invalidate(authToken);
      invalidatedTokens.put(authToken, DUMMY);
      invalidate = true;
    }
    return invalidate;
  }


}
