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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class PreparedStatementCache {

  private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementCache.class);

  //This class is created per operation.
  private final Connection connection;
  private final String tableName;
  private final List<JdbcFieldColumnMapping> generatedColumnMappings;
  private final List<String> primaryKeyColumns;
  private final int opCode;
  private final boolean caseSensitive;
  public static final int UNLIMITED_CACHE = -1;

  private final LoadingCache<SortedMap<String, String>, PreparedStatement> cacheMap;

  class PreparedStatementLoader extends CacheLoader<SortedMap<String, String>, PreparedStatement> {
    @Override
    public PreparedStatement load(SortedMap<String, String> columns) throws Exception {
      String query = generateQuery(columns);
      LOG.debug("Generated query: {}", query);

      PreparedStatement statement = JdbcUtil.getPreparedStatement(generatedColumnMappings, query, connection);
      return statement;
    }
  }

  class PreparedStatementRemovalListener implements RemovalListener<SortedMap<String, String>, PreparedStatement> {
    @Override
    public void onRemoval(RemovalNotification<SortedMap<String, String>, PreparedStatement> removal) {
      PreparedStatement stmt = removal.getValue();
      try {
        if (stmt != null){
          stmt.close();
        }
      } catch (SQLException ex) {
        LOG.error("Error while closing PreparedStatement evicted from cache. {}", ex);
      }
    }
  }

  PreparedStatementCache(Connection connection,
                         String tableName,
                         List<JdbcFieldColumnMapping> generatedColumnMappings,
                         List<String> primaryKeyColumns,
                         int opCode,
                         int maxCacheSize,
                         boolean caseSensitive)
  {
    this.connection = connection;
    this.tableName = tableName;
    this.generatedColumnMappings = generatedColumnMappings;
    this.primaryKeyColumns = primaryKeyColumns;
    this.opCode = opCode;
    this.caseSensitive = caseSensitive;

    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (maxCacheSize > -1){
      cacheBuilder.maximumSize(maxCacheSize);
    }

    cacheMap = cacheBuilder.removalListener(new PreparedStatementRemovalListener())
        .build(new PreparedStatementLoader());
  }

  PreparedStatement get(final SortedMap<String, String> columns) throws StageException {
    try {
      return cacheMap.get(columns);
    } catch (ExecutionException ex) {
      throw new StageException(JdbcErrors.JDBC_14, ex);
    }
  }

  private String generateQuery(final SortedMap<String, String> columns) throws OnRecordErrorException {
    List<String> primaryKeyParams = new LinkedList<>();
    for (String key: primaryKeyColumns) {
      primaryKeyParams.add(columns.get(key));
    }

    final int recordSize = 1;
    String query = JdbcUtil.generateQuery(opCode, tableName, primaryKeyColumns, primaryKeyParams, columns, recordSize, caseSensitive, false);
    LOG.debug("Generated single-row query:" + query);
    return query;
  }

  void destroy(){
    cacheMap.invalidateAll();
  }
}
