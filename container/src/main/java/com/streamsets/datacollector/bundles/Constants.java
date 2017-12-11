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

import com.amazonaws.util.Base64;

public final class Constants {

  // General

  /**
   * Configuration file that will be search in SDC_CONF for redactor configuration.
   */
  public static final String REDACTOR_CONFIG = "support-bundle-redactor.json";

  public static final String CUSTOMER_ID_FILE = "customer.id";
  public static final String DEFAULT_CUSTOMER_ID = "";

  /**
   * Uploading support bundles directly
   */
  public static final String UPLOAD_ENABLED = "bundle.upload.enabled";
  public static final boolean DEFAULT_UPLOAD_ENABLED = true;

  public static final String UPLOAD_BUCKET = "bundle.upload.bucket";
  public static final String DEFAULT_UPLOAD_BUCKET = "customer-support-bundles";

  public static final String UPLOAD_ACCESS = "bundle.upload.access";
  public static final String DEFAULT_UPLOAD_ACCESS = new String(Base64.decode("QUtJQUpPMllYVUJRVDZWSURFWEE="));

  public static final String UPLOAD_SECRET = "bundle.upload.secret";
  public static final String DEFAULT_UPLOAD_SECRET = new String(Base64.decode("NnNmU2ZzTEIwWWtnbThXd0RZeWVWR1VvdWxobW9lay9DNDI4YWEwcg=="));

  public static final String UPLOAD_BUFFER_SIZE = "bundle.upload.buffer_size";
  public static final int DEFAULT_UPLOAD_BUFFER_SIZE = 5 * 1024 * 1024; // 5MB, amazon lower limit

  public static final String UPLOAD_ON_ERROR = "bundle.upload.on_error";
  public static final boolean DEFAULT_UPLOAD_ON_ERROR = false;

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

  // Pipeline Generator

  /**
   * Redaction regular expression for pipeline configuration keys
   */
  public static final String PIPELINE_REDACT_REGEXP = "bundle.pipeline.redact_regexp";
  public static final String DEFAULT_PIPELINE_REDACT_REGEXP = "(.*[Pp]assword.*|.*AccessKey.*)";
}
