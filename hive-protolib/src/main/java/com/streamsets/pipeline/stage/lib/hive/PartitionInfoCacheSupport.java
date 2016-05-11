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
import com.google.common.cache.CacheBuilder;
import com.streamsets.pipeline.api.StageException;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Cache Support for Different set of Partition Values.
 */
public class PartitionInfoCacheSupport
    implements HMSCacheSupport<PartitionInfoCacheSupport.PartitionInfo,
    PartitionInfoCacheSupport.PartitionInfoCacheLoader> {

  @Override
  public PartitionInfoCacheLoader newHMSCacheLoader(String jdbcUrl, String qualifiedTableName) {
    return new PartitionInfoCacheLoader(jdbcUrl, qualifiedTableName);
  }

  @Override
  public Cache<String, Optional<PartitionInfo>> createCache(int maxCacheSize) {
    return CacheBuilder.<String, Optional<PartitionInfo>>newBuilder().maximumSize(maxCacheSize).build();
  }

  public static class PartitionInfo extends HMSCacheSupport.HMSCacheInfo<Set<LinkedHashMap<String,String>>>{
    public PartitionInfo(Set<LinkedHashMap<String,String>> partitionInfo) {
      super(partitionInfo);
    }

    @Override
    public Set<LinkedHashMap<String,String>> getDiff(Set<LinkedHashMap<String,String>> newState) throws StageException{
      Set<LinkedHashMap<String,String>> diff = new HashSet<>(newState);
      diff.removeAll(state);
      return diff;
    }

    @Override
    public void updateState(Set<LinkedHashMap<String,String>> newState) {
      state.addAll(newState);
    }
  }

  public class PartitionInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<PartitionInfo> {
    protected PartitionInfoCacheLoader(String jdbcUrl, String qualifiedTableName) {
      super(jdbcUrl, qualifiedTableName);
    }
    @Override
    protected PartitionInfo loadHMSCacheInfo() throws StageException{
      Set<LinkedHashMap<String, String>> partitionVals = executor.executeShowPartitionsQuery(qualifiedTableName);
      return new PartitionInfo(partitionVals);
    }
  }
}
