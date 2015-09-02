/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Source;

import java.util.List;
import java.util.Map;

/**
 * Cluster source which should be implemented by any source which can run in cluster mode
 */
public interface ClusterSource extends Source {

  /**
   * Writes batch of data to the source
   * @param batch
   * @throws InterruptedException
   */
  void put(List<Map.Entry> batch) throws InterruptedException;

  /**
   * Return the no of records produced by this source
   */
  long getRecordsProduced();

  /**
   * Return true if a unrecoverable error has occured
   */
  boolean inErrorState();

  /**
   * Returns name of this origin
   */
  String getName();

  /**
   * Whether source is configured to run in batch mode or not
   */
  boolean isInBatchMode();

  /**
   * The configs to ship to cluster
   */
  Map<String, String> getConfigsToShip();


  void shutdown();

  void postDestroy();
}
