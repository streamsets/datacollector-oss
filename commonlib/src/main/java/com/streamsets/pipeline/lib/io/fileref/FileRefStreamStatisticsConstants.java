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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io.fileref;

public final class FileRefStreamStatisticsConstants {
  private FileRefStreamStatisticsConstants() {}

  public static final String GAUGE_NAME = "File Transfer Statistics";
  public static final String FILE_NAME = "File Name";
  public static final String TRANSFER_THROUGHPUT = "Transfer Rate";
  public static final String COPIED_BYTES = "Copied Bytes";
  public static final String REMAINING_BYTES = "Remaining Bytes";
  public static final String TRANSFER_THROUGHPUT_METER = "transferRateKb";
}
