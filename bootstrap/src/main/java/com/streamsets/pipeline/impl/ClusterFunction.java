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
package com.streamsets.pipeline.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Common interface for cluster mode sources which push data.
 */
public interface ClusterFunction extends Serializable {

  /**
   * Invoke pipeline to start a batch. List passed to this interface
   * may not be reused after this call returns.
   *
   * @return The batch produced by the first Spark processor (or empty batch if none exist in the pipeline)
   */
  Iterator startBatch(List<Map.Entry> batch) throws Exception;

  /**
   * Set the number of spark processors in this pipeline.
   */
  void setSparkProcessorCount(int count);

  /**
   * Send the transformed batch to the i-th Spark processor in the pipeline.
   */
  Iterator forwardTransformedBatch(Iterator batch, int id) throws Exception;

  /**
   * Write errors to the i-th Spark processor in the pipeline.
   */
  void writeErrorRecords(Iterator errors, int id) throws Exception;

  /**
   * Shutdown all Embedded SDCs and thus all stages.
   */
  void shutdown() throws Exception;
}
