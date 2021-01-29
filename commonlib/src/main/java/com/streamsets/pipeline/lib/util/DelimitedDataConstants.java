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
package com.streamsets.pipeline.lib.util;

public class DelimitedDataConstants {
  public static final String PARSER = "parser";

  public static final String DELIMITER_CONFIG = "delimiterChar";
  public static final String ESCAPE_CONFIG = "escapeChar";
  public static final String QUOTE_CONFIG = "quoteChar";
  public static final String QUOTE_MODE = "quoteMode";
  public static final String SKIP_START_LINES = "skipStartLines";
  public static final String COMMENT_ALLOWED_CONFIG = "commentAllowed";
  public static final String COMMENT_MARKER_CONFIG = "commentMarker";
  public static final String IGNORE_EMPTY_LINES_CONFIG = "ignoreEmptyLines";
  public static final String PARSE_NULL = "parseNull";
  public static final String NULL_CONSTANT = "nullConstant";
  public static final String ALLOW_EXTRA_COLUMNS = "allowExtraColumns";
  public static final String EXTRA_COLUMN_PREFIX = "extraColumnPrefix";
  public static final String MULTI_CHARACTER_FIELD_DELIMITER_CONFIG = "multiCharacterFieldDelimiter";
  public static final String MULTI_CHARACTER_LINE_DELIMITER_CONFIG = "multiCharacterLineDelimiter";

  // Univocity Parser Configs
  public static final String UNIVOCITY_FIELD_SEPARATOR = "univocityFieldSeparator";
  public static final String UNIVOCITY_QUOTE = "univocityQuote";
  public static final String UNIVOCITY_ESCAPE = "univocityEscape";
  public static final String UNIVOCITY_MAX_COLUMNS = "univocityMaxColumns";
  public static final String UNIVOCITY_MAX_CHARS_PER_COLUMN = "univocityMaxCharsPerColumn";
  public static final String UNIVOCITY_SKIP_EMPTY_LINES = "univocitySkipEmptyLines";
  public static final String UNIVOCITY_COMMENT_CHAR = "univocityCommentChar";
  public static final String UNIVOCITY_LINE_SEPARATOR = "univocityLineSeparator";

  public static final String DEFAULT_EXTRA_COLUMN_PREFIX = "_extra_";

  public static final String DEFAULT_MULTI_CHARACTER_FIELD_DELIMITER = "||";
  public static final String DEFAULT_MULTI_CHARACTER_LINE_DELIMITER = "\n";
  public static final String DEFAULT_MULTI_CHARACTER_LINE_DELIMITER_EL = "${str:unescapeJava('\\\\n')}";

  private DelimitedDataConstants() {}
}
