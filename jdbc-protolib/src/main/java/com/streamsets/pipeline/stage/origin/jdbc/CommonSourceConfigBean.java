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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CommonSourceConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(CommonSourceConfigBean.class);
  public static final String DEFAULT_QUERIES_PER_SECONDS = "10";

  public CommonSourceConfigBean() {}

  @VisibleForTesting
  public CommonSourceConfigBean(String queriesPerSecond, int maxBatchSize, int maxClobSize, int maxBlobSize) {
    this(queriesPerSecond, maxBatchSize, maxClobSize, maxBlobSize, false);
  }

  @VisibleForTesting
  public CommonSourceConfigBean(String queriesPerSecond, int maxBatchSize, int maxClobSize, int maxBlobSize, boolean convertTimestampToString) {
    this.queriesPerSecond = queriesPerSecond;
    this.maxBatchSize = maxBatchSize;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.convertTimestampToString = convertTimestampToString;
    this.numSQLErrorRetries = 0;
  }

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = DEFAULT_QUERIES_PER_SECONDS,
      label = "Queries Per Second",
      description = "Number of queries that can be run per second by this source.  Set to zero for no limit.",
      displayPosition = 60,
      group = "JDBC"
  )
  public String queriesPerSecond = DEFAULT_QUERIES_PER_SECONDS;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (Records)",
      displayPosition = 140,
      group = "JDBC"
  )
  public int maxBatchSize;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Clob Size (Characters)",
      displayPosition = 150,
      group = "JDBC"
  )
  public int maxClobSize;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Blob Size (Bytes)",
      displayPosition = 151,
      group = "JDBC"
  )
  public int maxBlobSize;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Allow Late Tables",
      displayPosition = 170,
      group = "JDBC",
      defaultValue = "false"
  )
  public boolean allowLateTable = false;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${10 * SECONDS}",
      label = "New Table Discovery Interval",
      displayPosition = 171,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "JDBC",
      dependsOn = "allowLateTable",
      triggeredByValue = "true"
  )
  public long newTableQueryInterval;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable Schema Changes Event",
      displayPosition = 172,
      group = "JDBC"
  )
  public boolean enableSchemaChanges;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Transaction Length",
      description = "Time window to look for changes within a transaction before commit (in seconds)",
      displayPosition = 173,
      group = "JDBC",
      elDefs = TimeEL.class,
      defaultValue = "-1"
  )
  public int txnWindow;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "No-more-data Event Generation Delay (seconds)",
      description = "Number of seconds to delay when all rows have been processed, before generating the no-more-data" +
          " event.  Used if you want other events to show up in the event stream first.",
      displayPosition = 201,
      min = 0,
      group = "JDBC"
  )
  public int noMoreDataEventDelay;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Convert Timestamp To String",
      description = "Rather then representing timestamps as Data Collector DATETIME type, use String.",
      displayPosition = 210,
      group = "JDBC"
  )
  public boolean convertTimestampToString;

  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String MAX_CLOB_SIZE = "maxClobSize";
  private static final String MAX_BLOB_SIZE = "maxBlobSize";
  private static final String QUERIES_PER_SECOND = "queriesPerSecond";
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
    if (!NumberUtils.isNumber(queriesPerSecond)) {
      issues.add(context.createConfigIssue(
          Groups.JDBC.name(),
          COMMON_SOURCE_CONFIG_BEAN_PREFIX + QUERIES_PER_SECOND,
          JdbcErrors.JDBC_88,
          queriesPerSecond
      ));
    }
    return issues;
  }

  public RateLimiter creatQueryRateLimiter() {
    final BigDecimal rateLimit = new BigDecimal(queriesPerSecond);
    if (rateLimit.signum() < 1) {
      // negative or zero value; no rate limit
      return null;
    } else {
      return RateLimiter.create(rateLimit.doubleValue());
    }
  }

  public static void upgradeRateLimitConfigs(
      List<Config> configs,
      String pathToCommonSourceConfigBean,
      int numThreads
  ) {
    final String queryIntervalField = String.format("%s.queryInterval", pathToCommonSourceConfigBean);
    final String queriesPerSecondField = String.format("%s.queriesPerSecond", pathToCommonSourceConfigBean);
    Config queryIntervalConfig = UpgraderUtils.getAndRemoveConfigWithName(configs, queryIntervalField);
    if (queryIntervalConfig == null) {
      throw new IllegalStateException(String.format("Did not find %s in configs: %s", queryIntervalField, configs));
    }

    final Object queryIntervalValue = queryIntervalConfig.getValue();

    final BigDecimal queriesPerSecond = getQueriesPerSecondFromInterval(
        queryIntervalValue,
        numThreads,
        queriesPerSecondField,
        queryIntervalField
    );

    configs.add(new Config(queriesPerSecondField, queriesPerSecond.toPlainString()));
  }


  public static BigDecimal getQueriesPerSecondFromInterval(
      Object queryIntervalValue,
      int numThreads,
      String queriesPerSecondField,
      String queryIntervalField
  ) {
    if (numThreads <= 0) {
      LOG.warn("numThreads was given as {} in query rate limit upgrade; switching to default value of 1", numThreads);
      numThreads = 1;
    }

    BigDecimal queriesPerSecond = new BigDecimal(DEFAULT_QUERIES_PER_SECONDS);

    Long intervalSeconds = null;
    if (queryIntervalValue instanceof String) {
      final String queryIntervalExpr = (String) queryIntervalValue;

      // total hack, but we don't have access to a real EL evaluation within upgraders so we will only recognize
      // specific kinds of experssions (namely, the previous default value of ${10 * SECONDS}, or any similar
      // expression with a number other than 10
      final String secondsElExpr = "^\\s*\\$\\{\\s*([0-9]*)\\s*\\*\\s*SECONDS\\s*\\}\\s*$";

      final Matcher matcher = Pattern.compile(secondsElExpr).matcher(queryIntervalExpr);
      if (matcher.matches()) {
        final String secondsStr = matcher.group(1);
        intervalSeconds = Long.valueOf(secondsStr);
      } else if (StringUtils.isNumeric(queryIntervalExpr)) {
        intervalSeconds = Long.valueOf(queryIntervalExpr);
      } else {
        LOG.warn(
            "{} was a String, but was not a recognized format (either EL expression like '${N * SECONDS}' or simply" +
                " an integral number, so could not automatically convert it; will use a default value",
            queryIntervalValue
        );
      }
    } else if (queryIntervalValue instanceof Integer) {
      intervalSeconds = (long) ((Integer) queryIntervalValue).intValue();
    } else if (queryIntervalValue instanceof Long) {
      intervalSeconds = (Long)queryIntervalValue;
    }

    if (intervalSeconds != null) {
      if (intervalSeconds > 0) {
        // Force a double operation to not lose precision.
        // Don't use BigDecimal#divide() to avoid hitting ArithmeticException if numThreads/intervalSeconds
        // is non-terminating.
        queriesPerSecond = BigDecimal.valueOf(((double) numThreads)/intervalSeconds);
        LOG.info(
            "Calculated a {} value of {} from {} of {} and numThreads of {}",
            queriesPerSecondField,
            queriesPerSecond,
            queryIntervalField,
            queryIntervalValue,
            numThreads
        );
      } else {
        queriesPerSecond = BigDecimal.ZERO;
        LOG.warn(
            "{} value of {} was not positive, so had to use a value of {} for {}, which means unlimited",
            queryIntervalField,
            queryIntervalValue,
            queriesPerSecondField,
            queriesPerSecond
        );
      }
    } else {
      LOG.warn(
          "Could not calculate a {} value, so using a default value of {}",
          queriesPerSecondField,
          queriesPerSecond
      );
    }

    return queriesPerSecond;
  }

}
