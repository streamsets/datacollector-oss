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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.lib.hive;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.destination.hive.Errors;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A Write through cache which internally holds multiple caches one for each {@link HMSCacheType}
 * which it is supposed to support.
 */
public class HMSCache {
  private Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap;

  private HMSCache(Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap) {
    this.cacheMap = cacheMap;
  }

  /**
   * Use this method to get a {@link Builder} to build {@link HMSCache}.
   * @return {@link Builder}
   */
  public static Builder newCacheBuilder() {
    return new Builder();
  }

  /**
   * Returns if the {@link HMSCache} has the corresponding {@link HMSCacheSupport.HMSCacheInfo} and qualified table name.
   * @param hmsCacheType {@link HMSCacheType}
   * @param qualifiedTableName qualified table name
   * @param <T> {@link HMSCacheSupport.HMSCacheInfo}
   * @return Corresponding {@link HMSCacheSupport.HMSCacheInfo} for the qualified table name.
   * @throws StageException if the {@link HMSCacheType} is not supported by {@link HMSCache}
   */
  @SuppressWarnings("unchecked")
  public <T extends HMSCacheSupport.HMSCacheInfo> T getIfPresent(
      HMSCacheType hmsCacheType,
      String qualifiedTableName) throws StageException{
    if (!cacheMap.containsKey(hmsCacheType)) {
      throw new StageException(Errors.HIVE_16, hmsCacheType);
    }
    Optional<HMSCacheSupport.HMSCacheInfo> ret =cacheMap.get(hmsCacheType).getIfPresent(qualifiedTableName);
    return ret == null ? null : (T)ret.orNull();
  }

  /**
   * Returns if the {@link HMSCache} has the corresponding {@link HMSCacheSupport.HMSCacheInfo}
   * and qualified table name.<br>
   * If it is not there load it using corresponding {@link HMSCacheSupport.HMSCacheLoader}
   * @param <T> {@link HMSCacheSupport.HMSCacheInfo}
   * @param hmsCacheType {@link HMSCacheType}
   * @param jdbcUrl JDBC Url.
   * @param qualifiedTableName qualified table name
   * @param auxiliaryInfo Any auxiliary Info for {@link HMSCacheSupport.HMSCacheLoader}
   * @return Corresponding {@link HMSCacheSupport.HMSCacheInfo} for the qualified table name.
   * @throws StageException if the {@link HMSCacheType} is not supported by {@link HMSCache}
   */
  @SuppressWarnings("unchecked")
  public <T extends HMSCacheSupport.HMSCacheInfo> T getOrLoad(
      HMSCacheType hmsCacheType,
      String jdbcUrl, String qualifiedTableName,
      Object... auxiliaryInfo) throws StageException{
    if (!cacheMap.containsKey(hmsCacheType)) {
      throw new StageException(Errors.HIVE_16, hmsCacheType);
    }
    try {
      Optional<HMSCacheSupport.HMSCacheInfo> ret =
          cacheMap.get(hmsCacheType).get(
              qualifiedTableName,
              hmsCacheType.getSupport().newHMSCacheLoader(
                  jdbcUrl,
                  qualifiedTableName,
                  auxiliaryInfo
          )
      );
      return ret == null ? null : (T)ret.orNull();
    } catch(ExecutionException e) {
      throw new StageException(Errors.HIVE_01, e);
    }
  }

  /**
   * Puts/ updates the {@link HMSCache} with {@link HMSCacheSupport.HMSCacheInfo} for corresponding
   * {@link HMSCacheType} and qualified table name
   * @param cacheType {@link HMSCacheType}
   * @param qualifiedTableName qualified table name
   * @param hmsCacheInfo {@link HMSCacheSupport.HMSCacheInfo}
   * @param <T> {@link HMSCacheSupport.HMSCacheInfo}
   * @throws StageException if the {@link HMSCacheType} is not supported by {@link HMSCache}
   */
  public <T extends HMSCacheSupport.HMSCacheInfo> void put(
      HMSCacheType cacheType,
      String qualifiedTableName,
      T hmsCacheInfo
  ) throws StageException {
    if (!cacheMap.containsKey(cacheType)) {
      throw new StageException(Errors.HIVE_16, cacheType);
    }
    cacheMap.get(cacheType).put(qualifiedTableName, Optional.of((HMSCacheSupport.HMSCacheInfo)hmsCacheInfo));
  }

  /**
   * A builder for building {@link HMSCache}
   */
  public static final class Builder {
    Set<HMSCacheType> cacheTypes = new HashSet<>();
    int maxCacheSize = -1;

    private Builder() {}

    /**
     * Determines the underlying cache(s) size for {@link HMSCache}
     * @param maxCacheSize cache size
     * @return {@link Builder}
     */
    public Builder maxCacheSize(int maxCacheSize) {
      this.maxCacheSize = maxCacheSize > 0? maxCacheSize : Integer.MAX_VALUE;
      return this;
    }

    /**
     * Adds support to {@link HMSCacheType}
     * @param cacheType {@link HMSCacheType}
     * @return @return {@link Builder}
     */
    public Builder addCacheTypeSupport(HMSCacheType cacheType) {
      cacheTypes.add(cacheType);
      return this;
    }

    /**
     * Adds support to collection of {@link HMSCacheType}
     * @param cacheTypes Collection of {@link HMSCacheType}
     * @return @return {@link Builder}
     */
    public Builder addCacheTypeSupport(Collection<HMSCacheType> cacheTypes) {
      this.cacheTypes.addAll(cacheTypes);
      return this;
    }

    /**
     * Build instance of {@link HMSCache}
     * @return {@link HMSCache}
     */
    public HMSCache build() {
      Utils.checkArgument(!cacheTypes.isEmpty(), "Invalid HMSCache Configuration");
      Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap = new HashMap<>();
      for (HMSCacheType type : cacheTypes) {
        cacheMap.put(type, type.getSupport().createCache(maxCacheSize));
      }
      return new HMSCache(cacheMap);
    }
  }
}
