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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.exceptions.HiveStageCheckedException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cache Support for Different set of Partition Values.
 */
public class PartitionInfoCacheSupport
    implements HMSCacheSupport<PartitionInfoCacheSupport.PartitionInfo,
    PartitionInfoCacheSupport.PartitionInfoCacheLoader> {

  @Override
  public PartitionInfoCacheLoader newHMSCacheLoader(HiveQueryExecutor executor) {
    return new PartitionInfoCacheLoader(executor);
  }

  public static class PartitionValues {
    LinkedHashMap<String, String> partitionValues;

    public PartitionValues(LinkedHashMap<String, String> partitionValues) {
      this.partitionValues = partitionValues;
    }

    public LinkedHashMap<String, String> getPartitionValues() {
      return partitionValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PartitionValues that = (PartitionValues) o;

      return partitionValues != null ? partitionValues.equals(that.partitionValues) : that.partitionValues == null;

    }

    @Override
    public int hashCode() {
      return partitionValues != null ? partitionValues.hashCode() : 0;
    }
  }

  public static class PartitionInfo extends HMSCacheSupport.HMSCacheInfo<Map<PartitionValues, String>>{
    public PartitionInfo(Map<PartitionValues, String> partitionInfo) {
      super(partitionInfo);
    }

    public Map<PartitionValues, String> getPartitions() {
      return state;
    }

    @Override
    public Map<PartitionValues, String> getDiff(Map<PartitionValues, String> newState) throws StageException {
      Map<PartitionValues, String> diff = new HashMap<>();
      for (Map.Entry<PartitionValues, String> newEntry : newState.entrySet()) {
        if (!state.containsKey(newEntry.getKey())) {
          diff.put(newEntry.getKey(), newEntry.getValue());
        } else if(!state.get(newEntry.getKey()).equals(newEntry.getValue())) {
          throw new HiveStageCheckedException(Errors.HIVE_31, newEntry.getValue(), state.get(newEntry.getKey()));
        }
      }
      return diff;
    }

    @Override
    public void updateState(Map<PartitionValues, String> newState) {
      state.putAll(newState);
    }
  }

  public class PartitionInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<PartitionInfo> {
    protected PartitionInfoCacheLoader(HiveQueryExecutor executor) {
      super(executor);
    }
    @Override
    protected PartitionInfo loadHMSCacheInfo(String qualifiedTableName) throws StageException{
      Set<PartitionValues> partitionValSet = executor.executeShowPartitionsQuery(qualifiedTableName);
      Map<PartitionValues, String> partitionValuesToLocationMap = new HashMap<>();
      //As Per http://docs.oracle.com/javase/7/docs/api/java/sql/Statement.html#executeBatch%28%29
      //Can't executeBatch for queries with result set so has to live with serial execute for now.
      for (PartitionValues partitionVals : partitionValSet) {
        String location =
            executor.executeDescFormattedPartitionAndGetLocation(
                qualifiedTableName,
                partitionVals.getPartitionValues()
            );
        partitionValuesToLocationMap.put(partitionVals, location);
      }
      return new PartitionInfo(partitionValuesToLocationMap);
    }
  }
}
