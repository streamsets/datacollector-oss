/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Preconditions;

// Offset format: Filename::timestamp::offset. I miss case classes here.
class Offset {
  private static final String OFFSET_DELIMITER = "::";

  final String fileName;
  final long timestamp;

  private String offset;
  String offsetStr;

  Offset(String offsetStr) {
    String[] parts = offsetStr.split(OFFSET_DELIMITER);
    Preconditions.checkArgument(parts.length == 3);
    this.offsetStr = offsetStr;
    this.fileName = parts[0];
    this.timestamp = Long.parseLong(parts[1]);
    this.offset = parts[2];
  }

  Offset(String fileName, long timestamp, String offset) {
    this.fileName = fileName;
    this.offset = offset;
    this.timestamp = timestamp;
    this.offsetStr = getOffsetStr();
  }

  void setOffset(String offset) {
    this.offset = offset;
    this.offsetStr = getOffsetStr();
  }

  private String getOffsetStr() {
    return fileName + OFFSET_DELIMITER + timestamp + OFFSET_DELIMITER + offset;
  }

  public String getFileName() {
    return fileName;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getOffset() {
    return offset;
  }
}
