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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3Constants {
  static final String MINUS_ONE = "-1";
  static final String ZERO = "0";
  static final String EMPTY = "";
  static final String AMAZON_S3_THREAD_PREFIX = "Amazon S3 Runner - ";
  static final String AMAZON_S3_METRICS = "Amazon S3 Metrics for Thread - ";

  static final long DEFAULT_FETCH_SIZE = 1024 * 1024L;
  static final String BUCKET = "bucket";
  static final String OBJECT_KEY = "objectKey";
  static final String OWNER = "owner";
  static final String OFFSET = "Current Offset";
  static final String SIZE = "size";
  static final String CONTENT_LENGTH = "Content-Length";
  static final String THREAD_NAME = "Thread Name";
  static final String STATUS = "Status";

  private S3Constants() {}
}
