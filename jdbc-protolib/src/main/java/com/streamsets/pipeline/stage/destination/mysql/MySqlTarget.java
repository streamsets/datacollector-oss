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
package com.streamsets.pipeline.stage.destination.mysql;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.DuplicateKeyAction;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnParamMapping;
import com.streamsets.pipeline.lib.operation.ChangeLogFormat;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.lib.operation.UnsupportedOperationAction;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * MySQL/MemSQL Destination for StreamSets Data Collector.
 */
public class MySqlTarget extends JdbcTarget {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlTarget.class);

  public MySqlTarget(
      String schema,
      String tableNameTemplate,
      List<JdbcFieldColumnParamMapping> customMappings,
      DuplicateKeyAction duplicateKeyAction,
      HikariPoolConfigBean hikariConfigBean
  ) {
    super(
        schema,
        tableNameTemplate,
        customMappings,
        false, // Not require to enclose table names
        false, // No rollback support
        true, // Always use multi-row operation
        -1, // No statement limit
        -1, // Not applicable
        ChangeLogFormat.NONE,
        OperationType.LOAD_CODE,
        UnsupportedOperationAction.SEND_TO_ERROR,
        duplicateKeyAction,
        hikariConfigBean
    );
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    return issues;
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    super.write(batch);
  }
}
