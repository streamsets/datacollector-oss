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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.util.DirectedGraph;
import com.streamsets.pipeline.lib.jdbc.multithread.util.TopologicalSorter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Used to Provide table order based on referential constraints.
 */
public final class ReferentialTblOrderProvider extends TableOrderProvider.BaseTableOrderProvider {
  //Cache will hold a maximum of 1000 table references.
  private static final int MAX_REFERRED_TABLE_CACHE_SIZE = 1000;

  private final Connection connection;
  private final DirectedGraph<String> directedGraph;
  private final LoadingCache<String, Set<String>> referredTables;

  private volatile boolean areAllEdgesConstructed;
  private Queue<String> orderedTables;
  private final JdbcUtil jdbcUtil;


  public ReferentialTblOrderProvider(Connection conn) {
    this.jdbcUtil = UtilsProvider.getJdbcUtil();
    this.connection = conn;
    directedGraph = new DirectedGraph<>();
    referredTables = CacheBuilder.newBuilder().maximumSize(MAX_REFERRED_TABLE_CACHE_SIZE).build(new CacheLoader<String, Set<String>>() {
      @Override
      public Set<String> load(String key) throws SQLException {
        TableContext tableContext =  getTableContext(key);
        return jdbcUtil.getReferredTables(connection, tableContext.getSchema(), tableContext.getTableName());
      }
    });
    areAllEdgesConstructed = false;
    orderedTables = new LinkedList<>();
  }

  @Override
  public void addTable(String qualifiedTableName) {
    directedGraph.addVertex(qualifiedTableName);
    areAllEdgesConstructed = false;
  }

  @Override
  public Queue<String> calculateOrder() throws SQLException, ExecutionException, StageException {
    if (!areAllEdgesConstructed) {
      orderedTables = new LinkedList<>();
      for (String qualifiedTableName : directedGraph.vertices()) {
        Set<String> referredTableSetForThisContext = referredTables.get(qualifiedTableName);
        TableContext tableContext = getTableContext(qualifiedTableName);
        for (String referredTable : referredTableSetForThisContext) {
          TableContext referredTableContext = getTableContext(tableContext.getSchema(), referredTable);
          //Checking whether the referred table is used by the origin or has the table has a reference to itself.
          if (referredTableContext != null
              && !referredTableContext.getQualifiedName().equals(tableContext.getQualifiedName())) {
            //This edge states referred table should be ingested first
            directedGraph.addDirectedEdge(
                referredTableContext.getQualifiedName(),
                qualifiedTableName
            );
          }
        }
      }

      areAllEdgesConstructed = true;
      try {
        Iterator<String> topologicalOrderIterator =
            new TopologicalSorter<>(directedGraph, String::compareTo).sort().iterator();
        topologicalOrderIterator.forEachRemaining(table -> orderedTables.add(table));
      } catch(IllegalStateException e) {
        throw new StageException(JdbcErrors.JDBC_68, e.getMessage());
      }
    }

    //Return the saved topological order.
    return new LinkedList<>(orderedTables);
  }
}
