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
package com.streamsets.pipeline.lib.jdbc.multithread.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Listens for cache invalidation and appropriately closes the result set and statement.
 */
public class JdbcTableReadContextInvalidationListener implements RemovalListener<TableRuntimeContext, TableReadContext> {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableReadContextInvalidationListener.class);

  @Override
  public void onRemoval(RemovalNotification<TableRuntimeContext, TableReadContext> tableReadContextRemovalNotification) {
    Optional.ofNullable(tableReadContextRemovalNotification.getKey()).ifPresent(tableContext -> {
      LOG.debug("Closing statement and result set for : {}", tableContext.getQualifiedName());
      Optional.ofNullable(tableReadContextRemovalNotification.getValue()).ifPresent(readContext -> {
        readContext.resetProcessingMetrics();
        //Destroy and close statement/result set.
        readContext.destroy();
      });
    });
  }
}
