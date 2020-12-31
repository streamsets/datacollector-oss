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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.ConnectionManager;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import com.streamsets.pipeline.lib.jdbc.multithread.TableReadContext;
import com.streamsets.pipeline.lib.jdbc.multithread.TableRuntimeContext;
import com.streamsets.pipeline.lib.util.OffsetUtil;
import com.streamsets.pipeline.stage.origin.jdbc.AbstractTableJdbcSource;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableJdbcSource extends AbstractTableJdbcSource {
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.jdbc.table.TableJdbcSource.offset.version$";
  public static final String OFFSET_VERSION_1 = "1";
  public static final String OFFSET_VERSION_2 = "2";

  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSource.class);

  public TableJdbcSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean
  ) {
    this(hikariConfigBean, commonSourceConfigBean, tableJdbcConfigBean, UtilsProvider.getTableContextUtil());
  }

  public TableJdbcSource(
      HikariPoolConfigBean hikariConfigBean,
      CommonSourceConfigBean commonSourceConfigBean,
      TableJdbcConfigBean tableJdbcConfigBean,
      TableContextUtil tableContextUtil
  ) {
    super(hikariConfigBean, commonSourceConfigBean, tableJdbcConfigBean, tableContextUtil);
  }

  protected void checkConnectionAndBootstrap(Stage.Context context, List<ConfigIssue> issues) {
    super.checkConnectionAndBootstrap(context, issues);
  }

  @Override
  public void validateTableJdbcConfigBean(PushSource.Context context, DatabaseVendor vendor, List<ConfigIssue> issues) {
    issues = tableJdbcConfigBean.validateConfigs(context, vendor, issues);

    //Max pool size should be equal to number of threads
    //The main thread will use one connection to list threads and close (return to hikari pool) the connection
    // and each individual data threads needs one connection
    if (tableJdbcConfigBean.numberOfThreads > hikariConfigBean.maximumPoolSize) {
      issues.add(
          getContext().createConfigIssue(
              com.streamsets.pipeline.stage.origin.jdbc.table.Groups.ADVANCED.name(),
              "hikariConfigBean." + HikariPoolConfigBean.MAX_POOL_SIZE_NAME,
              JdbcErrors.JDBC_74,
              hikariConfigBean.maximumPoolSize,
              tableJdbcConfigBean.numberOfThreads
          )
      );
    }
  }

  @Override
  protected CacheLoader<TableRuntimeContext, TableReadContext> getTableReadContextCache(
      ConnectionManager connectionManager,
      Map<String, String> offsets
  ) {
    return null;
  }

  @Override
  protected Map<String, TableContext> listTablesForConfig(PushSource.Context context, List<ConfigIssue> issues, ConnectionManager connectionManager) throws SQLException, StageException {
    Map<String, TableContext> allTableContexts = new HashMap<>();
    for (TableConfigBean tableConfigBean : tableJdbcConfigBean.getTableConfigs()) {
      //No duplicates even though a table matches multiple configurations, we will add it only once.
      allTableContexts.putAll(
          tableContextUtil.listTablesForConfig(
          hikariConfigBean.getVendor(),
          getContext(),
          issues,
          connectionManager.getConnection(),
          tableConfigBean,
          new TableJdbcELEvalContext(context, context.createELVars()),
          tableJdbcConfigBean.quoteChar
      ));
    }

    return allTableContexts;
  }

  @VisibleForTesting
  @Override
  protected void handleLastOffset(Map<String, String> lastOffsets) throws StageException {
    if (lastOffsets != null) {
      String offsetVersion;
      //Only if it is not already committed
      if (lastOffsets.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
        String innerTableOffsetsAsString = lastOffsets.get(Source.POLL_SOURCE_OFFSET_KEY);

        if (innerTableOffsetsAsString != null) {
          try {
            getOffsets().putAll(OffsetUtil.deserializeOffsetMap(innerTableOffsetsAsString));
          } catch (IOException ex) {
            LOG.error("Error when deserializing", ex);
            throw new StageException(JdbcErrors.JDBC_61, ex);
          }
        }

        getOffsets().forEach((tableName, tableOffset) -> getContext().commitOffset(tableName, tableOffset));

        upgradeFromV1();

        //Remove Poll Source Offset key from the offset.
        //Do this at last so as not to lose the offsets if there is failure in the middle
        //when we call commitOffset above
        getContext().commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);
      } else {
        offsetVersion = lastOffsets.remove(OFFSET_VERSION);

        if (OFFSET_VERSION_2.equals(offsetVersion)) {
          final Map<String, String> newCommitOffsets = new HashMap<>();
          getTableOrderProvider().initializeFromV2Offsets(lastOffsets, newCommitOffsets);

          //clear out existing offset keys and recommit new ones
          for (String offsetKey : lastOffsets.keySet()) {
            if (OFFSET_VERSION.equals(offsetKey)) {
              continue;
            }
            getContext().commitOffset(offsetKey, null);
          }
          getOffsets().putAll(newCommitOffsets);
          newCommitOffsets.forEach((key, value) -> getContext().commitOffset(key, value));
        } else if (OFFSET_VERSION_1.equals(offsetVersion) || Strings.isNullOrEmpty(offsetVersion)) {
          getOffsets().putAll(lastOffsets);
          upgradeFromV1();
        }
      }

      //Version the offset so as to allow for future evolution.
      getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_2);

    }
  }

  private void upgradeFromV1() throws StageException {
    final Set<String> offsetKeysToRemove = getTableOrderProvider().initializeFromV1Offsets(getOffsets());
    if (offsetKeysToRemove != null && offsetKeysToRemove.size() > 0) {
      LOG.info("Removing now outdated offset keys: {}", offsetKeysToRemove);
      offsetKeysToRemove.forEach(tableName -> getContext().commitOffset(tableName, null));
    }
  }
}
