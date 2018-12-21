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
package com.streamsets.pipeline.lib.dirspooler;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.OffsetUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Offset {
  public static final String VERSION_ONE = "1";
  private static final String OFFSET_SEPARATOR = "::";
  public static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";
  public static final String NULL_FILE = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c";
  private static final String POS = "POS";
  private final String file;
  private String fileOffset;

  public Offset(String version, String offsetString) throws StageException {
    if (version.equals(VERSION_ONE)) {
      this.file = getFileFromSourceOffset(offsetString);
      this.fileOffset = getOffsetFromSourceOffset(offsetString);
    } else {
      throw new IllegalArgumentException("mismatch version : " + version);
    }
  }

  public Offset(String version, String file, String offset) throws StageException {
    this.file = file;

    if (version.equals(VERSION_ONE)) {
      try {
        if (offset != null && offset.startsWith("{")) {
          Map<String, String> map = OffsetUtil.deserializeOffsetMap(offset);
          this.fileOffset = map.get(POS) == null ? ZERO : map.get(POS);
        } else {
          this.fileOffset = offset;
        }
      } catch (IOException ex) {
        throw new StageException(Errors.SPOOLDIR_34, ex.getCause(), ex);
      }
    } else {
      throw new IllegalArgumentException("mismatch version : " + version);
    }
  }

  public String getOffsetString() throws StageException {
    Map<String, String> map = new HashMap<>();
    map.put(POS, getOffset());

    try {
      return OffsetUtil.serializeOffsetMap(map);
    } catch (IOException ex) {
      throw new StageException(Errors.SPOOLDIR_33, ex.toString(), ex);
    }
  }

  public String getOffset() {
    if (fileOffset != null && fileOffset.isEmpty()) {
      return MINUS_ONE;
    }

    return fileOffset;
  }

  public String getRawFile() {
    return file;
  }

  public String getFile() {
    if (file == null) {
      return NULL_FILE;
    }

    return file;
  }

  @VisibleForTesting
  String getFileFromSourceOffset(String sourceOffset) throws StageException {
    String sourceOffsetFile = null;
    if (sourceOffset != null && sourceOffset.length() > 0) {
      sourceOffsetFile = sourceOffset;
      int separator = sourceOffsetFile.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        sourceOffsetFile = sourceOffsetFile.substring(0, separator);
        if (NULL_FILE.equals(sourceOffsetFile)) {
          sourceOffsetFile = null;
        }
      }
    }
    return sourceOffsetFile;
  }

  @VisibleForTesting
  String getOffsetFromSourceOffset(String sourceOffset) throws StageException {
    String offset = ZERO;
    if (sourceOffset != null && sourceOffset.length() > 0) {
      int separator = sourceOffset.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        offset = sourceOffset.substring(separator + OFFSET_SEPARATOR.length());
      }

      if (offset == null || offset.length() < 1 ) {
        offset = ZERO;
      }
    }
    return offset;
  }
}
