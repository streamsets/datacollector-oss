/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.hbase.api;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

public interface HBaseProcessor {

  /**
   * Fetches the HBaseConnectionHelper
   *
   * @return HBaseConnectionHelper
   */
  HBaseConnectionHelper getHBaseConnectionHelper();

  /**
   * Creates a HBase table
   *
   * @throws InterruptedException
   * @throws IOException
   */
  void createTable() throws InterruptedException, IOException;

  /**
   * Destroy the existing HBaseTable
   */
  void destroyTable();

  /**
   * Returns the result of the get operation
   *
   * @param get Get operation
   * @return result
   *
   * @throws IOException
   */
  Result get(Get get) throws IOException;

  /**
   * Returns the results of the get operations
   *
   * @param gets List of get operation
   * @return Array of results
   *
   * @throws IOException
   */
  Result[] get(List<Get> gets) throws IOException;


}
