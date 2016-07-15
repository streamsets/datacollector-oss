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
package com.streamsets.pipeline.lib.parser.wholefile;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WholeFileDataParserFactory extends DataParserFactory {
  static final ImmutableMap<String, ?> MANDATORY_METADATA_INFO =
      ImmutableMap
          .of("file", String.class)
          .of("filename", String.class)
          .of("owner", String.class)
          .of("group", String.class)
          .of("lastModifiedTime", Long.class)
          .of("lastAccessTime", Long.class)
          .of("creationTime", Long.class)
          .of("size", Long.class);

  public static final Map<String, Object> CONFIGS = new HashMap<>();
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public WholeFileDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataParser getParser(
      String id,
      Map<String, Object> metadata,
      FileRef fileRef
  ) throws DataParserException {
    Utils.checkNotNull(fileRef, "fileRef");
    validateMetadata(metadata);
    return new WholeFileDataParser(
        getSettings().getContext(),
        id,
        metadata,
        fileRef
    );
  }

  private static void validateMetadata(Map<String, Object> metadata) throws DataParserException {
    if (!metadata.keySet().containsAll(MANDATORY_METADATA_INFO.keySet())) {
      Set<String> missingMetadata = new HashSet<>(MANDATORY_METADATA_INFO.keySet());
      missingMetadata.removeAll(metadata.keySet());
      StringBuilder sb = new StringBuilder();
      boolean commaNeeded = false;
      for (String missing : missingMetadata ) {
        if (commaNeeded) {
          sb.append(", ");
        }
        sb.append(missing);
        commaNeeded = true;
      }
      throw new DataParserException(Errors.WHOLE_FILE_PARSER_ERROR_0, sb.toString());
    }
    boolean isValid = true;
    StringBuilder sb = new StringBuilder();
    for (String metadataKey : MANDATORY_METADATA_INFO.keySet()) {
      Object metadataObject = metadata.get(metadataKey);
      Class<?> classType = (Class<?>) MANDATORY_METADATA_INFO.get(metadataKey);
      if (!classType.isAssignableFrom(metadataObject.getClass())) {
        sb.append(
            Utils.format(
                " Property : {}, Expected Type: {}, Actual Type: {}",
                metadataKey,
                classType.getName(),
                metadataObject.getClass().getName()
            )
        );
        sb.append("\n");
        isValid = false;
      }
    }
    if (!isValid) {
      throw new DataParserException(Errors.WHOLE_FILE_PARSER_ERROR_1, sb.toString());
    }
  }
}
