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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.streamsets.pipeline.api.StageException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_204;

public class RawTypeHandler {

  private static final Pattern HEX_TO_RAW_PATTERN = Pattern.compile("HEXTORAW\\('(.*)'\\)");

  public static byte[] parseRaw(String column, String value, int columnType) throws StageException {
    if (value == null) {
      return null;
    }
    Matcher m = HEX_TO_RAW_PATTERN.matcher(value);
    if (m.find()) {
      try {
        return Hex.decodeHex(m.group(1).toCharArray());
      } catch (DecoderException e) {
        throw new StageException(JDBC_204, m.group(1));
      }
    }
    throw new UnsupportedFieldTypeException(column, value, columnType);
  }
}