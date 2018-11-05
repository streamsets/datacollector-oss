/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.pipeline.lib.jdbc.multithread.TableContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(UtilsProvider.class);

  private static JdbcUtil jdbcUtil = new JdbcUtil();
  private static TableContextUtil tableContextUtil = new TableContextUtil();

  // Do not allow to create instances of this class
  private UtilsProvider() {

  }

  public static JdbcUtil getJdbcUtil() {
    return jdbcUtil;
  }

  public static void setJdbcUtil(JdbcUtil jdbcUtil) {
    LOG.info("Setting custom JdbcUtil '{}' in classloader '{}'",
        jdbcUtil.getClass(),
        jdbcUtil.getClass().getClassLoader()
    );
    UtilsProvider.jdbcUtil = jdbcUtil;
  }

  public static TableContextUtil getTableContextUtil() {
    return tableContextUtil;
  }

  public static void setTableContextUtil(TableContextUtil tableContextUtil) {
    LOG.info(
        "Setting custom TableContextUtil '{}' in classloader '{}'",
        tableContextUtil.getClass(),
        tableContextUtil.getClass().getClassLoader()
    );
    UtilsProvider.tableContextUtil = tableContextUtil;
  }

}
