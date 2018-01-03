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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.util.regex.Matcher;

public class CEFParser extends ExtendedFormatParser {

  private static final char CEF_DEFAULT_SEP = ' ';

  public CEFParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      int maxObjectLen,
      boolean retainOriginalText,
      GenericObjectPool<StringBuilder> currentLineBuilderPool,
      GenericObjectPool<StringBuilder> previousLineBuilderPool
  ) throws IOException {
    super(context,
        readerId,
        reader,
        readerOffset,
        maxObjectLen,
        retainOriginalText,
        "CEF",
        ExtendedFormatType.CEF,
        currentLineBuilderPool,
        previousLineBuilderPool
    );
  }

  @Override
  public Field getExtFormatVersion(String val) {
    int cefVersion = Integer.parseInt(StringUtils.substringAfter(val, ":"));
    return Field.create(cefVersion);
  }

  @Override
  public int getNumHeaderFields(ExtendedFormatType formatType, Field formatVersion) {
    return 7;
  }

  @Override
  public String getHeaderFieldName(int index) {
    switch (index) {
      case 1:
        return "vendor";
      case 2:
        return "product";
      case 3:
        return "version";
      case 4:
        return "signature";
      case 5:
        return "name";
      case 6:
        return "severity";
    }
    throw new IllegalArgumentException("Invalid header index (" + index + ") while parsing record");
  }

  @Override
  protected char getExtensionAttrSeparator(Matcher m, int index, StringBuilder logLine) {
    return CEF_DEFAULT_SEP;
  }
}
