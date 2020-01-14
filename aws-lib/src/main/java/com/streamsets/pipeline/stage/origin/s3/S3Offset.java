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

package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;

public class S3Offset implements Serializable {
  private final String key;
  private final String eTag;
  private final String offset;
  private final String timestamp;

  static final String OFFSET_SEPARATOR = "::";
  private static final String ZERO = "0";

  S3Offset(String key, String offset, String eTag, String timestamp) {
    this.key = key;
    this.offset = offset;
    this.eTag = eTag;
    this.timestamp = timestamp;
  }

  S3Offset(S3Offset offset) {
    this.key = offset.getKey();
    this.offset = offset.getOffset();
    this.eTag = offset.geteTag();
    this.timestamp = offset.getTimestamp();
  }

  S3Offset(S3Offset offset, String offsetValue) {
    this.key = offset.getKey();
    this.offset = offsetValue;
    this.eTag = offset.geteTag();
    this.timestamp = offset.getTimestamp();
  }

  public String getKey() {
    return key;
  }

  String geteTag() {
    return eTag;
  }

  String getOffset() {
    return offset;
  }

  String getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    // Excel Data Format's offset is "sheet name::offset". We don't expect offset to include "::" so we'll modify a bit
    return key +
        OFFSET_SEPARATOR +
        offset.replaceAll("::", ":\\\\:") +
        OFFSET_SEPARATOR +
        eTag +
        OFFSET_SEPARATOR +
        timestamp;
  }

  static S3Offset fromString(String lastSourceOffset) throws StageException {

    if (lastSourceOffset != null) {
      String savedOffset = lastSourceOffset.replaceAll(":\\\\:", "::");
      String[] split = savedOffset.split(OFFSET_SEPARATOR);
      if (split.length == 5) {
        // Excel Data Format's offset is "sheet name::offset".
        return new S3Offset(split[0], Utils.format("{}::{}", split[1], split[2]), split[3], split[4]);
      } else if (split.length == 4) {
        return new S3Offset(split[0], split[1], split[2], split[3]);
      } else {
        throw new StageException(Errors.S3_SPOOLDIR_21, lastSourceOffset);
      }
    }
    return new S3Offset(null, ZERO, null, ZERO);
  }

  public boolean representsFile() {
    return !getKey().equals(S3Constants.EMPTY) ||
        !getOffset().equals(S3Constants.ZERO) ||
        !geteTag().equals(S3Constants.EMPTY) ||
        !getTimestamp().equals(S3Constants.ZERO);
  }
}