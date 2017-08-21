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
package com.streamsets.datacollector.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EscapeUtil {
  public static final Pattern pattern = Pattern.compile("\\W+?", Pattern.CASE_INSENSITIVE);
  public static final char QUOTE_CHAR = '\'';

  private EscapeUtil() {}

  public static String singleQuoteEscape(String path) {
    // Skip escaping if no non-word chars are found
    // This is likely slower than just escaping it anyway
    // but currently left as-is for compatibility
    if (path == null || !pattern.matcher(path).find()) {
      return path;
    }
    return escapeQuotesAndBackSlash(path, true);
  }

  public static String singleQuoteUnescape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find() && path.length() > 2) {
        path = unescapeQuotesAndBackSlash(path, true);
        return path.substring(1, path.length() - 1);
      }
    }
    return path;
  }

  public static String doubleQuoteEscape(String path) {
    if(path == null || !pattern.matcher(path).find()) {
      return path;
    }
    return escapeQuotesAndBackSlash(path, false);
  }

  public static String doubleQuoteUnescape(String path) {
    if(path != null) {
      Matcher matcher = pattern.matcher(path);
      if(matcher.find() && path.length() > 2) {
        path = unescapeQuotesAndBackSlash(path, false);
        return path.substring(1, path.length() - 1);
      }
    }
    return path;
  }

  /**
   * This method is used during deserializer and sqpath (Single quote escaped path) is passed to determine the last field name.
   */
  public static String getLastFieldNameFromPath(String path) {
    String [] pathSplit = (path != null) ? path.split("/") : null;
    if(pathSplit != null && pathSplit.length > 0) {
      String lastFieldName = pathSplit[pathSplit.length - 1];

      //handle special case field name containing slash eg. /'foo/bar'
      boolean singleQuoted = lastFieldName.charAt(0) == QUOTE_CHAR
          && lastFieldName.charAt(lastFieldName.length() - 1) == QUOTE_CHAR;

      if(lastFieldName.contains("'") && !singleQuoted) {
        //If path contains slash inside name, split it by "/'"
        pathSplit = path.split("/'");
        if(pathSplit.length > 0) {
          lastFieldName = "'" + pathSplit[pathSplit.length - 1];
          singleQuoted = true;
        }
      }

      if (singleQuoted) {
        return singleQuoteUnescape(lastFieldName);
      } else {
        return lastFieldName;
      }
    }
    return path;
  }

  /**
   * This method escapes backslash, double quotes and single quotes (keeping replacement of ' to \\\\\'
   * as is so as to maintain backward compatibility any serialization/deserialization)
   */
  private static String escapeQuotesAndBackSlash(String path, boolean isSingleQuoteEscape) {
    String quoteChar = isSingleQuoteEscape? "'" : "\"";
    StringBuilder sb = new StringBuilder(path.length() * 2).append(quoteChar);
    char[] chars = path.toCharArray();
    for (char c : chars) {
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c == '"') {
        sb.append(isSingleQuoteEscape? "\\\"" : "\\\\\"");
      } else if (c == '\'') {
        sb.append(isSingleQuoteEscape? "\\\\\'" : "\\\'");
      } else {
        sb.append(c);
      }
    }
    return sb.append(quoteChar).toString();
  }


  /**
   * This method un escapes backslash, double quotes and single quotes (keeping replacement of \\\\\' to '
   * as is so as to maintain backward compatibility any serialization/deserialization)
   */
  private static String unescapeQuotesAndBackSlash(String path, boolean isSingleQuoteUnescape) {
    path = (isSingleQuoteUnescape)? path.replace("\\\"", "\"").replace("\\\\\'", "'")
        : path.replace("\\\\\"", "\"").replace("\\\'", "'");
    return path.replace("\\\\", "\\");
  }

  /**
   * This method un escapes backslash and un escapes extra escapes before double quotes and single quotes
   * (appended by {@link #escapeQuotesAndBackSlash(String, boolean)}).
   * This method should be used internally and not during for any serialization/deserialization
   */
  public static String standardizePathForParse(String path, boolean isSingleQuoteEscape) {
    path = isSingleQuoteEscape? path.replace("\\\\\'", "\\'") : path.replace("\\\\\"", "\\\"");
    return path.replace("\\\\", "\\");
  }
}
