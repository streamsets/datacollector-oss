/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.StageException;

@FunctionalInterface
public interface JdbcTableCreator {

  /**
   * Creates the sql table if it does not exist yet
   *
   * @param schema the Schema name where the table belongs to
   * @param table the table name
   * @return true if table is created, false otherwise. If table already exists it should return false.
   * @throws StageException Exception thrown when running the sql create table query another exception is thrown
   */
  boolean create(String schema, String table) throws StageException;

}
