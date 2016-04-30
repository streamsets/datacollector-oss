/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.common;

/**
 * Shared header constants for keys that are generated into the header attributes by
 * various origins.
 */
public class HeaderAttributeConstants {

  /**
   * Full file path to the source file.
   *
   * Applicable for LogTail, Directory spooling, ...
   */
  public static final String FILE = "file";

  /**
   * Offset in the source.
   *
   * Can be file offset in directory spooling, log tail or for example partition offset
   * in Kafka source.
   */
  public static final String OFFSET = "offset";

}
