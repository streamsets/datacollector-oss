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
import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;

/**
 * This is a generic Cache Supporter interface for {@link HMSCacheType}.<br>
 * Consumers should implement {@link HMSCacheSupport} for each of the new
 * {@link HMSCacheType} that is added.
 * @param <IN> {@link HMSCacheInfo} that will be stored inside the cache.
 * @param <CL> {@link HMSCacheLoader} for loading the {@link HMSCacheInfo} into the cache.
 */
public interface HMSCacheSupport<IN extends HMSCacheSupport.HMSCacheInfo,
    CL extends HMSCacheSupport.HMSCacheLoader<IN>> {

  /**
   * Creates a new {@link HMSCacheLoader}
   * @param executor {@link HiveQueryExecutor}
   * @return new {@link HMSCacheLoader}
   */
  CL newHMSCacheLoader(HiveQueryExecutor executor) throws StageException;

  /**
   * A Cache information which should be extended for each
   * {@link HMSCacheType}.
   * The internal {@link #state} will represent internal state object maintained by the cache.
   * @param <T> The real object which will be stored inside the cache.
   */
  abstract class HMSCacheInfo<T> {
    protected T state;

    HMSCacheInfo(T state) {
      this.state = state;
    }

    /**
     * Returns the diff between current state with another state.
     * @param anotherState the another state which should be diff with the current information.
     * @return the diff
     */
    abstract T getDiff(T anotherState) throws StageException;

    /**
     * Updates the internal state.
     * @param newState the new State which should be updated/merged/replaced.
     */
    abstract void updateState(T newState) throws StageException;

  }

  /**
   * A Cache Loader which is responsible for loading the corresponding {@link HMSCacheInfo} into the cache,
   * @param <IN> the corresponding Cache Information {@link HMSCacheInfo} for this particular loader.
   */
  abstract class HMSCacheLoader<IN extends HMSCacheInfo> extends CacheLoader<String, Optional<IN>> {
    protected final HiveQueryExecutor executor;

    protected HMSCacheLoader(HiveQueryExecutor executor){
      this.executor = executor;
    }

    /**
     * Load the corresponding {@link HMSCacheInfo}
     * @return {@link HMSCacheInfo}
     * @throws StageException if there is an issue in loading.
     */
    protected abstract IN loadHMSCacheInfo(String qualifiedTableName) throws StageException;

    @Override
    public Optional<IN> load(String qualifiedTableName) throws StageException {
      boolean doesTableExist = executor.executeShowTableQuery(qualifiedTableName);
      return doesTableExist ? Optional.of(loadHMSCacheInfo(qualifiedTableName)) : Optional.<IN>absent();
    }
  }
}
