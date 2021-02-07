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
package com.streamsets.datacollector.bundles;

public final class Constants {

  // General

  /**
   * Configuration file that will be search in SDC_CONF for redactor configuration.
   */
  public static final String REDACTOR_CONFIG = "support-bundle-redactor.json";

  public static final String CUSTOMER_ID_FILE = "customer.id";
  public static final String DEFAULT_CUSTOMER_ID = "";

  // Log Generator

  /**
   * 2GB of raw logs is equal to roughly ~70MB after zip compression (on real life logs)
   */
  public static final String LOG_MAX_SIZE = "bundle.log.max_size";
  public static final long DEFAULT_LOG_MAX_SIZE = 2L * (1024 * 1024 * 1024);

  /**
   * For GC, we want last ~50 MBs (random constant at this point).
   */
  public static final String LOG_GC_MAX_SIZE = "bundle.log.gc_max_size";
  public static final long DEFAULT_LOG_GC_MAX_SIZE = (50 * 1024 * 1024);

  /**
   * For JVM FATAL error file, we want last ~10 MBs (random constant at this point).
   */
  public static final String LOG_HS_MAX_SIZE = "bundle.log.hs_max_size";
  public static final long DEFAULT_LOG_HS_MAX_SIZE = (10 * 1024 * 1024);

  // Pipeline Generator

  /**
   * Redaction regular expression for pipeline configuration keys
   */
  public static final String PIPELINE_REDACT_REGEXP = "bundle.pipeline.redact_regexp";
  public static final String DEFAULT_PIPELINE_REDACT_REGEXP = "(.*[Pp]assword.*|.*AccessKey.*)";

  // Sdc Info Generator

  /**
   * Historical ThreadDump configuration
   */
  public static final String HISTORICAL_THREAD_DUMP_COUNT = "bundle.info.thread_dump.historical.count";
  public static final int DEFAULT_HISTORICAL_THREAD_DUMP_COUNT = 5;

  public static final String HISTORICAL_THREAD_DUMP_PERIOD = "bundle.info.thread_dump.historical.period.seconds";
  public static final long DEFAULT_HISTORICAL_THREAD_DUMP_PERIOD = 5 * 60; // 5 minutes

  public static final String LIVE_THREAD_DUMP_COUNT = "bundle.info.thread_dump.live.count";
  public static final int DEFAULT_LIVE_THREAD_DUMP_COUNT = 3;

  public static final String LIVE_THREAD_DUMP_PERIOD = "bundle.info.thread_dump.live.period";
  public static final long DEFAULT_LIVE_THREAD_DUMP_PERIOD = 1 * 1000; // 1 seconds
}
