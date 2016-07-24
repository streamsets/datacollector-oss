/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.lib.util;

public class DelimitedDataConstants {

  public static final String DELIMITER_CONFIG = "delimiterChar";
  public static final String ESCAPE_CONFIG = "escapeChar";
  public static final String QUOTE_CONFIG = "quoteChar";
  public static final String SKIP_START_LINES = "skipStartLines";
  public static final String PARSE_NULL = "parseNull";
  public static final String NULL_CONSTANT = "nullConstant";

  private DelimitedDataConstants() {}
}
