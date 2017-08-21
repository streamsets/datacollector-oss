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
package com.streamsets.pipeline.stage.lib.hive.cache;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Write through cache which internally holds multiple caches one for each {@link HMSCacheType}
 * which it is supposed to support.
 */
public class HMSCache {
  /**
   * Internal map with all the sub-caches for various different entities
   */
  private Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap;

  /**
   * Locks for tables to make sure that we don't call loading queries on the same table twice.
   */
  private final LoadingCache<String, Lock> tableLocks;

  private HMSCache(Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap) {
    this.cacheMap = cacheMap;

    // Locks inside will expire 15 minutes -- idea is that if a thread holds a lock for more then 15 minutes,
    // then we have bigger issues to worry about than that two threads could be querying the same table twice.
    this.tableLocks = CacheBuilder.newBuilder()
      .expireAfterAccess(15, TimeUnit.MINUTES)
      .build(new CacheLoader<String, Lock>() {
        @Override
        public Lock load(String key) throws Exception {
          return new ReentrantLock();
        }
      });
  }

  /**
   * Use this method to get a {@link Builder} to build {@link HMSCache}.
   * @return {@link Builder}
   */
  public static Builder newCacheBuilder() {
    return new Builder();
  }

  private static String keyForLockMap(HMSCacheType type, String qualifiedTableName) {
    return type.name() + "_" + qualifiedTableName;
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
      String qualifiedTableName
  ) throws StageException {
    if (!cacheMap.containsKey(hmsCacheType)) {
      throw new StageException(Errors.HIVE_16, hmsCacheType);
    }
    Optional<HMSCacheSupport.HMSCacheInfo> ret = cacheMap.get(hmsCacheType).getIfPresent(qualifiedTableName);
    return ret == null ? null : (T)ret.orNull();
  }

  /**
   * Returns if the {@link HMSCache} has the corresponding {@link HMSCacheSupport.HMSCacheInfo}
   * and qualified table name. This method is safe to call from multiple threads - it will block and serialize
   * multiple readers. It guarantees that each value will be loaded at most once to limit load on HS2 as much
   * as possible.
   *
   * If it is not there load it using corresponding {@link HMSCacheSupport.HMSCacheLoader}
   *
   * @param <T> {@link HMSCacheSupport.HMSCacheInfo}
   * @param hmsCacheType {@link HMSCacheType}
   * @param qualifiedTableName qualified table name
   * @return Corresponding {@link HMSCacheSupport.HMSCacheInfo} for the qualified table name.
   * @throws StageException if the {@link HMSCacheType} is not supported by {@link HMSCache}
   */
  @SuppressWarnings("unchecked")
  public <T extends HMSCacheSupport.HMSCacheInfo> T getOrLoad(
      HMSCacheType hmsCacheType,
      String qualifiedTableName,
      HiveQueryExecutor queryExecutor
  ) throws StageException {
    if (!cacheMap.containsKey(hmsCacheType)) {
      throw new StageException(Errors.HIVE_16, hmsCacheType);
    }

    // Firstly validate if the data already exists to avoid locking
    T cacheValue = getIfPresent(hmsCacheType, qualifiedTableName);
    if(cacheValue != null) {
      return cacheValue;
    }

    // For altering operation, get exclusive lock
    Lock lock = null;
    try {
      lock = tableLocks.get(keyForLockMap(hmsCacheType, qualifiedTableName));
      lock.lock();

      // Check the presence again as another thread could load the value before we got the lock
      cacheValue = getIfPresent(hmsCacheType, qualifiedTableName);
      if(cacheValue != null) {
        return cacheValue;
      }

      // Load the value from Hive
      return (T)(cacheMap.get(hmsCacheType).get(
        qualifiedTableName,
        () -> hmsCacheType.getSupport().newHMSCacheLoader(queryExecutor).load(qualifiedTableName)
      )).orNull();
    } catch(ExecutionException e) {
      throw new StageException(Errors.HIVE_01, e);
    } finally {
      if(lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Puts/ updates the {@link HMSCache} with {@link HMSCacheSupport.HMSCacheInfo} for corresponding
   * {@link HMSCacheType} and qualified table name.
   *
   * This method skips any locking provided by getOrLoad() and hence the caller code should not mix getOrLoad() and
   * put() calls in the same code.
   *
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
    cacheMap.get(cacheType).put(qualifiedTableName, Optional.of(hmsCacheInfo));
  }

  public <T extends HMSCacheSupport.HMSCacheInfo> void invalidate(
      HMSCacheType cacheType,
      String qualifiedTableName
  ) throws StageException {
    if (!cacheMap.containsKey(cacheType)) {
      throw new StageException(Errors.HIVE_16, cacheType);
    }
    cacheMap.get(cacheType).invalidate(qualifiedTableName);
  }

  /**
   * A builder for building {@link HMSCache}
   */
  public static final class Builder {
    Set<HMSCacheType> cacheTypes = new HashSet<>();
    long maxCacheSize = -1;

    private Builder() {}

    /**
     * Determines the underlying cache(s) size for {@link HMSCache}
     * @param maxCacheSize cache size
     * @return {@link Builder}
     */
    public Builder maxCacheSize(long maxCacheSize) {
      this.maxCacheSize = maxCacheSize;
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
    @SuppressWarnings("unchecked")
    public HMSCache build() throws StageException {
      Utils.checkArgument(
          !cacheTypes.isEmpty(),
          "Invalid HMSCache Configuration, Should support at least one type of cache"
      );

      Map<HMSCacheType, Cache<String, Optional<HMSCacheSupport.HMSCacheInfo>>> cacheMap = new HashMap<>();
      CacheBuilder cacheBuilder = CacheBuilder.newBuilder();

      if (maxCacheSize > 0) {
        cacheBuilder.maximumSize(maxCacheSize);
      }

      for (HMSCacheType type : cacheTypes) {
        cacheMap.put(type, cacheBuilder.build());
      }
      return new HMSCache(cacheMap);
    }
  }
}
