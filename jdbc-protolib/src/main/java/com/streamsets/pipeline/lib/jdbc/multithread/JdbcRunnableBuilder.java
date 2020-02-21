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

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.SQLServerCDCSource;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcConfigBean;

import java.util.Map;

public class JdbcRunnableBuilder {
  protected PushSource.Context context;
  protected int threadNumber;
  protected int batchSize;
  protected TableJdbcConfigBean tableJdbcConfigBean;
  protected CommonSourceConfigBean commonSourceConfigBean;
  protected Map<String, String> offsets;
  protected ConnectionManager connectionManager;
  protected MultithreadedTableProvider tableProvider;
  protected CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache;
  protected RateLimiter queryRateLimiter;
  protected boolean isReconnect;
  protected Map<String, SQLServerCDCSource.SourceTableInfo> infoMap;

  public JdbcRunnableBuilder() {
  }

  public JdbcRunnableBuilder context(PushSource.Context context) {
    this.context = context;
    return this;
  }

  public JdbcRunnableBuilder threadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
    return this;
  }

  public JdbcRunnableBuilder batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public JdbcRunnableBuilder offsets(Map<String, String> offsets) {
    this.offsets = offsets;
    return this;
  }

  public JdbcRunnableBuilder tableProvider(MultithreadedTableProvider tableProvider) {
    this.tableProvider = tableProvider;
    return this;
  }

  public JdbcRunnableBuilder connectionManager(ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
    return this;
  }

  public JdbcRunnableBuilder tableJdbcConfigBean(TableJdbcConfigBean tableJdbcConfigBean) {
    this.tableJdbcConfigBean = tableJdbcConfigBean;
    return this;
  }

  public JdbcRunnableBuilder commonSourceConfigBean(CommonSourceConfigBean commonSourceConfigBean) {
    this.commonSourceConfigBean = commonSourceConfigBean;
    return this;
  }

  public JdbcRunnableBuilder tableReadContextCache(CacheLoader<TableRuntimeContext, TableReadContext> tableReadContextCache) {
    this.tableReadContextCache = tableReadContextCache;
    return this;
  }

  public JdbcRunnableBuilder queryRateLimiter(RateLimiter queryRateLimiter) {
    this.queryRateLimiter = queryRateLimiter;
    return this;
  }

  public JdbcRunnableBuilder isReconnect(boolean isReconnect) {
    this.isReconnect = isReconnect;
    return this;
  }

  public JdbcRunnableBuilder sourceTableInfo(Map<String, SQLServerCDCSource.SourceTableInfo> infoMap) {
    this.infoMap = infoMap;
    return this;
  }

  public JdbcBaseRunnable build() {
    final String SQLServerCT = "SQLServerChangeTrackingClient";
    final String SQLServerCDC = "SQLServerCDCClient";

    if (context.getStageInfo().getInstanceName().startsWith(SQLServerCT)) {
      return new CTJdbcRunnable(
          context,
          threadNumber,
          batchSize,
          offsets,
          tableProvider,
          connectionManager,
          tableJdbcConfigBean,
          commonSourceConfigBean,
          tableReadContextCache,
          queryRateLimiter
      );
    } else if (context.getStageInfo().getInstanceName().startsWith(SQLServerCDC)) {
      return new CDCJdbcRunnable(
          context,
          threadNumber,
          batchSize,
          offsets,
          tableProvider,
          connectionManager,
          tableJdbcConfigBean,
          commonSourceConfigBean,
          tableReadContextCache,
          queryRateLimiter,
          isReconnect,
          infoMap
      );
    } else {
      return new TableJdbcRunnable(
          context,
          threadNumber,
          batchSize,
          offsets,
          tableProvider,
          connectionManager,
          tableJdbcConfigBean,
          commonSourceConfigBean,
          tableReadContextCache,
          queryRateLimiter
      );
    }
  }
}
