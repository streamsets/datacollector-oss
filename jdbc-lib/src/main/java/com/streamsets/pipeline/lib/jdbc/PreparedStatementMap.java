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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.StageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.SortedMap;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

final class PreparedStatementMap {

  /**
   * Map of opCode and PreparedStatementCache.
   */
  private final Map<Integer, PreparedStatementCache> cache = new HashMap<>();

  public PreparedStatementMap(
      Connection connection,
      String tableName,
      List<JdbcFieldColumnMapping> generatedColumnMappings,
      List<String> primaryKeyColumns,
      int maxPrepStmtCache,
      boolean caseSensitive)
  {
    for (JDBCOperationType type: JDBCOperationType.values()) {
      cache.put(type.code, new PreparedStatementCache(
          connection,
          tableName,
          generatedColumnMappings,
          primaryKeyColumns,
          type.code,
          maxPrepStmtCache,
          caseSensitive)
      );
    }
  }

  /**
   * Receive operation code and columnName-param map per each record.
   * Return a PreparedStatement if cache already has one for the same operation code
   * and same number of columns. Otherwise load one.
   * @param opCode Operation Code
   * @param columns Column to param mapping.
   * @return PreparedStatement
   * @throws StageException
   */
  @VisibleForTesting
  PreparedStatement getPreparedStatement(int opCode, SortedMap<String, String> columns)
      throws StageException {
    //Cache already has PreparedStatementCache for all opCode.
    if (!cache.containsKey(opCode)){
      // This check has been done earlier, so shouldn't come here.
      throw new StageException(JdbcErrors.JDBC_70, opCode);
    }
    return cache.get(opCode).get(columns);
  }

  void destroy(){
    for (Map.Entry<Integer, PreparedStatementCache> pcache: cache.entrySet()){
      pcache.getValue().destroy();
    }
  }
}
