/**
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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;

import java.util.List;

public final class CommonSourceConfigBean {

  public CommonSourceConfigBean() {}

  @VisibleForTesting
  public CommonSourceConfigBean(long queryInterval, int maxBatchSize, int maxClobSize, int maxBlobSize) {
    this.queryInterval = queryInterval;
    this.maxBatchSize = maxBatchSize;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.numSQLErrorRetries = 0;
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${10 * SECONDS}",
      label = "Query Interval",
      displayPosition = 60,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "JDBC"
  )
  public long queryInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (Records)",
      displayPosition = 140,
      group = "JDBC"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Clob Size (Characters)",
      displayPosition = 150,
      group = "JDBC"
  )
  public int maxClobSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Blob Size (Bytes)",
      displayPosition = 151,
      group = "JDBC"
  )
  public int maxBlobSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Number of Retries on SQL Error",
      description = "The number of retries upon a SQL Error.  To allow for the possibility of" +
          " handling transient errors (ex: connection issues, deadlock, etc.), provide a positive value here.  After" +
          " the specified number of retries is reached, the stage will fail upon the next error and pipeline error" +
          " handling behavior will take over.",
      min = 0,
      displayPosition = 160,
      group = "JDBC"
  )
  public int numSQLErrorRetries;

  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String MAX_CLOB_SIZE = "maxClobSize";
  private static final String MAX_BLOB_SIZE = "maxBlobSize";
  public static final String NUM_SQL_ERROR_RETRIES = "numSQLErrorRetries";
  public static final String COMMON_SOURCE_CONFIG_BEAN_PREFIX = "commonSourceConfigBean.";

  public List<Stage.ConfigIssue> validateConfigs(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (maxBatchSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), COMMON_SOURCE_CONFIG_BEAN_PREFIX + MAX_BATCH_SIZE, JdbcErrors.JDBC_10, maxBatchSize, 0));
    }
    if (maxClobSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), COMMON_SOURCE_CONFIG_BEAN_PREFIX + MAX_CLOB_SIZE, JdbcErrors.JDBC_10, maxClobSize, 0));
    }
    if (maxBlobSize < 0) {
      issues.add(context.createConfigIssue(Groups.ADVANCED.name(), COMMON_SOURCE_CONFIG_BEAN_PREFIX + MAX_BLOB_SIZE, JdbcErrors.JDBC_10, maxBlobSize, 0));
    }
    return issues;
  }
}
