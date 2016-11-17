/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.table;

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
   * Returns number of tables being selected for the Table Order Provider
   * @return Number of tables
   */
  int getNumberOfTables();


  /**
   * Returns the next table in the order.
   * @return Next table in the order.
   */
  TableContext nextTable() throws SQLException, ExecutionException, StageException;

  abstract class BaseTableOrderProvider implements TableOrderProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableOrderProvider.class);
    private static final Joiner JOINER = Joiner.on("\n");

    private Map<String, TableContext> tableContextMap;
    private Queue<String> tableQueue;

    @Override
    public void initialize(Map<String, TableContext> allTableContexts) throws SQLException, ExecutionException, StageException {
      tableContextMap = allTableContexts;
      for (String qualifiedTableName : allTableContexts.keySet()) {
        addTable(qualifiedTableName);
      }
      tableQueue = calculateOrGetOrder();
      LOGGER.info("Ordering of Tables : \n {}", JOINER.join(tableQueue));
    }

    TableContext getTableContext(String schema, String tableName) {
      return tableContextMap.get(TableContextUtil.getQualifiedTableName(schema, tableName));
    }

    TableContext getTableContext(String qualifiedTableName) {
      return tableContextMap.get(qualifiedTableName);
    }

    @Override
    public int getNumberOfTables() {
      return tableContextMap.keySet().size();
    }

    @Override
    public TableContext nextTable() throws SQLException, ExecutionException, StageException {
      if (tableQueue.isEmpty()) {
        tableQueue = calculateOrGetOrder();
      }
      return tableContextMap.get(tableQueue.poll());
    }
    abstract Queue<String> calculateOrGetOrder() throws SQLException, ExecutionException, StageException;

    abstract void addTable(String qualifiedTableName);
  }
}
