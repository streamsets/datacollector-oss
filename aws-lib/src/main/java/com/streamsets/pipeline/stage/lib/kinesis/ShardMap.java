/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.kinesis;

import java.util.HashMap;

public class ShardMap {
  private final HashMap<String, String> map = new HashMap<>();

  /**
   * Adds a shardId for a particular partitionKey to the map and returns
   * whether or not we should re-count the shards in the stream.
   * @param partitionKey partitionKey for a UserRecord
   * @param shardId The shardId returned by a UserRecordResult
   * @return whether or not the caller should invalidate their shard count.
   */
  public boolean put(String partitionKey, String shardId) {
    boolean shouldInvalidate = false;
    String predictedShard = map.get(partitionKey);
    if (predictedShard != null && !shardId.equals(predictedShard)) {
      shouldInvalidate = true;
      map.clear();
    }

    map.put(partitionKey, shardId);
    return shouldInvalidate;
  }
}
