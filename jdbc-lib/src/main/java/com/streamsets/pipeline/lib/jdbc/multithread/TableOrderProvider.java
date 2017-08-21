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

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.StageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public interface TableOrderProvider {
  /**
   * Initialize the TableOrderProvider
   * @param allTableContexts table contexts for the configuration
   */
  void initialize(Map<String, TableContext> allTableContexts) throws SQLException, ExecutionException, StageException;

  /**
   * Returns the ordered queue of tables
   * @return The ordered list of tables.
   */
  Queue<String> getOrderedTables() throws SQLException, ExecutionException, StageException;

  abstract class BaseTableOrderProvider implements TableOrderProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TableOrderProvider.class);
    private static final Joiner NEW_LINE_JOINER = Joiner.on("\n");

    private Map<String, TableContext> tableContextMap;
    private Queue<String> tableQueue;

    @Override
    public void initialize(Map<String, TableContext> allTableContexts) throws SQLException, ExecutionException, StageException {
      tableContextMap = allTableContexts;
      allTableContexts.keySet().forEach(this::addTable);
      tableQueue = calculateOrder();
      LOG.info("Ordering of Tables : \n {}", NEW_LINE_JOINER.join(tableQueue));
    }

    TableContext getTableContext(String schema, String tableName) {
      return tableContextMap.get(TableContextUtil.getQualifiedTableName(schema, tableName));
    }

    TableContext getTableContext(String qualifiedTableName) {
      return tableContextMap.get(qualifiedTableName);
    }

    @Override
    public Queue<String> getOrderedTables() throws SQLException, ExecutionException, StageException {
     return tableQueue;
    }

    abstract Queue<String> calculateOrder() throws SQLException, ExecutionException, StageException;

    abstract void addTable(String qualifiedTableName);
  }
}
