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
import com.streamsets.pipeline.stage.destination.hive.Errors;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cache Support for Type information (Column and Partition).
 */
public class TypeInfoCacheSupport
    implements HMSCacheSupport<TypeInfoCacheSupport.TypeInfo,
    TypeInfoCacheSupport.TypeInfoCacheLoader> {

  @Override
  public TypeInfoCacheLoader newHMSCacheLoader(String jdbcUrl, String qualifiedTableName) {
    return new TypeInfoCacheLoader(jdbcUrl, qualifiedTableName);
  }

  @Override
  public Cache<String, Optional<TypeInfo>> createCache(int maxCacheSize) {
    return CacheBuilder.<String, Optional<TypeInfo>>newBuilder().maximumSize(maxCacheSize).build();
  }

  public static class TypeInfo extends HMSCacheSupport.HMSCacheInfo<LinkedHashMap<String, HiveType>>{

    /**
     * Well this could be multiplexed, but we may support different partition types in the future
     * so keeping this separate for each table.
     */
    private LinkedHashMap<String, HiveType> partitionTypeInfo;

    public TypeInfo(LinkedHashMap<String, HiveType> columnInfo, LinkedHashMap<String, HiveType> partitionTypeInfo) {
      super(columnInfo);
      this.partitionTypeInfo = partitionTypeInfo;
    }

    /*
     * We will not allow modifications.
     */
    public Map<String, HiveType> getPartitionTypeInfo() {
      return Collections.unmodifiableMap(partitionTypeInfo);
    }

    @Override
    public LinkedHashMap<String, HiveType> getDiff(LinkedHashMap<String, HiveType> newState) throws StageException{
      LinkedHashMap<String, HiveType> columnDiff = new LinkedHashMap<>();
      for (String columnName : newState.keySet()) {
        HiveType columnType = newState.get(columnName);
        if (!state.containsKey(columnName)) {
          columnDiff.put(columnName, columnType);
        } else if (state.get(columnName) != columnType) {
          throw new StageException(Errors.HIVE_21, state.get(columnName), columnType);
        }
      }
      return columnDiff;
    }

    @Override
    public void updateState(LinkedHashMap<String, HiveType> newColumnNameType) {
      state.putAll(newColumnNameType);
    }
  }

  public class TypeInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<TypeInfo> {
    protected TypeInfoCacheLoader(String jdbcUrl, String qualifiedTableName) {
      super(jdbcUrl, qualifiedTableName);
    }
    @Override
    protected TypeInfo loadHMSCacheInfo() throws StageException{
      Pair<LinkedHashMap<String, HiveType>, LinkedHashMap<String, HiveType>>  typeInfo =
          executor.executeDescTableQuery(qualifiedTableName);
      return new TypeInfo(typeInfo.getLeft(), typeInfo.getRight());
    }
  }
}
