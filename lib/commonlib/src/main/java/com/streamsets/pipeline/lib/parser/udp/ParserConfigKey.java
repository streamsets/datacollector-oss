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
package com.streamsets.pipeline.lib.parser.udp;

public enum ParserConfigKey {
  CHARSET,
  CONVERT_TIME,
  TYPES_DB_PATH,
  EXCLUDE_INTERVAL,
  AUTH_FILE_PATH,
  RAW_DATA_MODE,
  RAW_DATA_MULTIPLE_VALUES_BEHAVIOR,
  RAW_DATA_OUTPUT_FIELD_PATH,
  RAW_DATA_SEPARATOR_BYTES,
  NETFLOW_OUTPUT_VALUES_MODE,
  NETFLOW_MAX_TEMPLATE_CACHE_SIZE,
  NETFLOW_TEMPLATE_CACHE_TIMEOUT_MS
}
