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
package com.streamsets.pipeline.stage.lib.hive.cache;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Cache Support for TBLProperties Information.
 */
public class TBLPropertiesInfoCacheSupport
    implements HMSCacheSupport<TBLPropertiesInfoCacheSupport.TBLPropertiesInfo,
    TBLPropertiesInfoCacheSupport.TBLPropertiesInfoCacheLoader> {

  @Override
  public TBLPropertiesInfoCacheLoader newHMSCacheLoader(
      String jdbcUrl,
      String qualifiedTableName,
      Object... auxiliaryInfo
  ) {
    return new TBLPropertiesInfoCacheLoader(jdbcUrl, qualifiedTableName);
  }

  @Override
  public Cache<String, Optional<TBLPropertiesInfo>> createCache(int maxCacheSize) {
    return CacheBuilder.<String, Optional<TBLPropertiesInfo>>newBuilder().maximumSize(maxCacheSize).build();
  }

  public static class TBLPropertiesInfo extends HMSCacheSupport.HMSCacheInfo<Pair<Boolean, Boolean>> {
    public TBLPropertiesInfo(boolean isExternal, boolean useAsAvro) {
      super(Pair.of(isExternal, useAsAvro));
    }

    public TBLPropertiesInfo(Pair<Boolean, Boolean> state) {
      super(state);
    }

    public boolean isExternal() {
      return state.getLeft();
    }

    public boolean isUseAsAvro() {
      return state.getRight();
    }

    @Override
    Pair<Boolean, Boolean> getDiff(Pair<Boolean, Boolean> anotherState) throws StageException {
      throw new StageException(Errors.HIVE_01, anotherState, "Invalid operation");
    }

    @Override
    void updateState(Pair<Boolean, Boolean> newState) throws StageException {
      throw new StageException(Errors.HIVE_01, newState, "Invalid operation");
    }
  }

  public class TBLPropertiesInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<TBLPropertiesInfo> {
    protected TBLPropertiesInfoCacheLoader(String jdbcUrl, String qualifiedTableName) {
      super(jdbcUrl, qualifiedTableName);
    }

    @Override
    protected TBLPropertiesInfo loadHMSCacheInfo() throws StageException {
      return new TBLPropertiesInfo(executor.executeShowTBLPropertiesQuery(qualifiedTableName));
    }
  }
}
