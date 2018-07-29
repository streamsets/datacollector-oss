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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public final class TableOrderProviderFactory {

  private final Connection connection;
  private final TableOrderStrategy tableOrderStrategy;

  public TableOrderProviderFactory(Connection connection, TableOrderStrategy tableOrderStrategy) {
    this.connection = connection;
    this.tableOrderStrategy = tableOrderStrategy;
  }

  /**
   * Create {@link TableOrderProvider} for the {@link TableOrderStrategy}
   * @return {@link TableOrderProvider}
   */
  public TableOrderProvider create() {
    switch (tableOrderStrategy) {
      case NONE:
        //Don't do any ordering, just add tables as per the incoming order.
        return new DefaultTableOrderProvider(new LinkedHashSet<>());
      case ALPHABETICAL:
        //Alphabetical sorting based on table qualified names.
        return new DefaultTableOrderProvider(new TreeSet<>());
      case REFERENTIAL_CONSTRAINTS:
        return new ReferentialTblOrderProvider(connection);
      default:
        throw new IllegalArgumentException(Utils.format("Unknown table order strategy: {}", tableOrderStrategy));
    }
  }

  /**
   * Create a table order provider based on the table name ordering (either first in order or alphabetical order)
   */
  private static final class DefaultTableOrderProvider extends TableOrderProvider.BaseTableOrderProvider {
    final Set<String> orderedTableContexts;

    DefaultTableOrderProvider(Set<String> orderedTableContexts) {
      this.orderedTableContexts = orderedTableContexts;
    }

    @Override
    public Queue<String> calculateOrder() throws SQLException, ExecutionException, StageException {
      return new LinkedList<>(orderedTableContexts);
    }

    @Override
    public void addTable(String qualifiedTableName) {
      orderedTableContexts.add(qualifiedTableName);
    }
  }
}
