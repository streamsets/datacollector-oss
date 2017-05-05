/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


  // Log Generator

  /**
   * 1GB of raw logs is equal to roughly ~80MB after zip compression (depending on the type of logs)
   */
  public static final String LOG_MAX_SIZE = "bundle.log.max_size";
  public static final long DEFAULT_LOG_MAX_SIZE = (1024 * 1024 * 1024);

  /**
   * For GC, we want last ~50 MBs (random constant at this point).
   */
  public static final String LOG_GC_MAX_SIZE = "bundle.log.gc_max_size";
  public static final long DEFAULT_LOG_GC_MAX_SIZE = (50 * 1024 * 1024);

  // Pipeline Generator

  /**
   * Redaction regular expression for pipeline configuration keys
   */
  public static final String PIPELINE_REDACT_REGEXP = "bundle.pipeline.redact_regexp";
  public static final String DEFAULT_PIPELINE_REDACT_REGEXP = ".*[Pp]assword.*";
}
