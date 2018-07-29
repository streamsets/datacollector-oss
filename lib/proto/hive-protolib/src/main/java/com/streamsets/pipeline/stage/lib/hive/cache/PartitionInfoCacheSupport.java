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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cache Support for Different set of Partition Values.
 */
public class PartitionInfoCacheSupport
    implements HMSCacheSupport<PartitionInfoCacheSupport.PartitionInfo,
    PartitionInfoCacheSupport.PartitionInfoCacheLoader> {

  private static final String LOCATION_NOT_LOADED = "LOCATION_NOT_LOADED";

  @Override
  public PartitionInfoCacheLoader newHMSCacheLoader(HiveQueryExecutor executor) {
    return new PartitionInfoCacheLoader(executor);
  }

  public static class PartitionValues {
    // Key is partition column name
    // Value is the action partition value
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
    private final HiveQueryExecutor hiveQueryExecutor;
    private String qualifiedTableName;
    private Set<PartitionValues> partitionsToBeRolled;

    public PartitionInfo(
        Map<PartitionValues, String> partitionInfo,
        final HiveQueryExecutor hiveQueryExecutor,
        final String qualfiedTableName
    ) {
      super(partitionInfo);
      this.partitionsToBeRolled = new HashSet<>();
      this.hiveQueryExecutor = hiveQueryExecutor;
      this.qualifiedTableName = qualfiedTableName;
    }

    public Map<PartitionValues, String> getPartitions() {
      return state;
    }

    public void setAllPartitionsToBeRolled() {
      this.partitionsToBeRolled.addAll(state.keySet());
    }

    public boolean shouldRoll(PartitionValues partition) {
      return this.partitionsToBeRolled.remove(partition);
    }

    @Override
    public Map<PartitionValues, String> getDiff(Map<PartitionValues, String> newState) throws StageException {
      Map<PartitionValues, String> diff = new HashMap<>();
      for (Map.Entry<PartitionValues, String> newEntry : newState.entrySet()) {
        PartitionValues partitionVals = newEntry.getKey();
        String locationFromTheRecord = newEntry.getValue();
        if (!state.containsKey(partitionVals)) {
          diff.put(partitionVals, locationFromTheRecord);
        } else  {
          String fetchedLocationFromHive = state.get(partitionVals);
          //NOT LOADED, let's load it
          if (LOCATION_NOT_LOADED.equals(fetchedLocationFromHive)) {
            fetchedLocationFromHive =
                hiveQueryExecutor.executeDescFormattedPartitionAndGetLocation(
                    qualifiedTableName,
                    partitionVals.getPartitionValues()
                );
            state.put(partitionVals, fetchedLocationFromHive);
          }
          if(!locationFromTheRecord.equals(fetchedLocationFromHive)) {
            throw new HiveStageCheckedException(Errors.HIVE_31, locationFromTheRecord, fetchedLocationFromHive);
          }
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
      for (PartitionValues partitionVals : partitionValSet) {
        partitionValuesToLocationMap.put(partitionVals, LOCATION_NOT_LOADED);
      }
      return new PartitionInfo(partitionValuesToLocationMap, executor, qualifiedTableName);
    }
  }
}
