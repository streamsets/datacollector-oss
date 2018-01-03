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

public class LEEFParser extends ExtendedFormatParser {
  private static final char LEEF_DEFAULT_SEP = '\t';

  public LEEFParser(
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
        "LEEF",
        ExtendedFormatType.LEEF,
        currentLineBuilderPool,
        previousLineBuilderPool
    );
  }

  @Override
  public Field getExtFormatVersion(String val) {
    double leefVersion = Double.parseDouble(StringUtils.substringAfter(val, ":"));
    return Field.create(leefVersion);
  }

  @Override
  public int getNumHeaderFields(ExtendedFormatType formatType, Field formatVersion) {
    return 5;
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
        return "eventId";
    }
    throw new IllegalArgumentException("Invalid header index (" + index + ") while parsing record");
  }

  @Override
  protected char getExtensionAttrSeparator(Matcher m, int index, StringBuilder logLine) {
    char attrSep = LEEF_DEFAULT_SEP;
    if (m.find()) {
      String val = logLine.substring(index, m.start());
      if (val.length() == 1) {
        attrSep = val.charAt(0);
      } else {
        int hexStart = val.indexOf('x');
        if (hexStart == -1) {
          attrSep = (char)Integer.parseInt(val, 16);
        } else {
          attrSep = (char)Integer.parseInt(val.substring(hexStart+1), 16);
        }
      }
    }
    return attrSep;
  }
}
