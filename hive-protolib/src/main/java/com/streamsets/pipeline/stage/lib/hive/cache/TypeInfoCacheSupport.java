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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
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
  public TypeInfoCacheLoader newHMSCacheLoader(HiveQueryExecutor executor) {
    return new TypeInfoCacheLoader(executor);
  }

  public static class TypeInfo extends HMSCacheSupport.HMSCacheInfo<LinkedHashMap<String, HiveTypeInfo>>{

    /**
     * Well this could be multiplexed, but we may support different partition types in the future
     * so keeping this separate for each table.
     */
    private LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo;

    public TypeInfo(LinkedHashMap<String, HiveTypeInfo> columnInfo, LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo) {
      super(columnInfo);
      this.partitionTypeInfo = partitionTypeInfo;
    }

    /*
     * We will not allow modifications.
     */
    public Map<String, HiveTypeInfo> getPartitionTypeInfo() {
      return Collections.unmodifiableMap(partitionTypeInfo);
    }

    public LinkedHashMap<String, HiveTypeInfo> getColumnTypeInfo() {
      return state;
    }

    @Override
    public LinkedHashMap<String, HiveTypeInfo> getDiff(LinkedHashMap<String, HiveTypeInfo> newState) throws StageException{
      LinkedHashMap<String, HiveTypeInfo> columnDiff = new LinkedHashMap<>();
      for (Map.Entry<String, HiveTypeInfo> entry : newState.entrySet()) {
        String columnName = entry.getKey();
        HiveTypeInfo columnTypeInfo = entry.getValue();
        if (!state.containsKey(columnName)) {
          columnDiff.put(columnName, columnTypeInfo);
        } else if (!state.get(columnName).equals(columnTypeInfo)) {
          throw new HiveStageCheckedException(
              Errors.HIVE_21,
              columnName,
              state.get(columnName).toString(),
              columnTypeInfo.toString()
          );
        }
      }
      return columnDiff;
    }

    @Override
    public void updateState(LinkedHashMap<String, HiveTypeInfo> newColumnNameType) {
      state.putAll(newColumnNameType);
    }
  }

  public class TypeInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<TypeInfo> {
    protected TypeInfoCacheLoader(HiveQueryExecutor executor) {
      super(executor);
    }
    @Override
    protected TypeInfo loadHMSCacheInfo(String qualifiedTableName) throws StageException{
      Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>>  typeInfo =
          executor.executeDescTableQuery(qualifiedTableName);
      return new TypeInfo(typeInfo.getLeft(), typeInfo.getRight());
    }
  }
}
