/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.api.impl;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public final class Utils {

  Utils() {
  }

  public static <T> T checkNotNull(T value, Object varName) {
    if (value == null) {
      throw new NullPointerException(format("{} cannot be null", varName));
    }
    return value;
  }

  public static void checkArgument(boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalArgumentException((msg != null) ? msg.toString() : "");
    }
  }

  public static void checkState(boolean expression, Object msg) {
    if (!expression) {
      throw new IllegalStateException((msg != null) ? msg.toString() : "");
    }
  }

  // we cache a splitted version of the templates to speed up formatting
  private static final Map<String, String[]> TEMPLATES = new ConcurrentHashMap<>();

  private static final String TOKEN = "{}";

  static String[] prepareTemplate(String template) {
    List<String> list = new ArrayList<>();
    int pos = 0;
    int nextToken = template.indexOf(TOKEN, pos);
    while (nextToken > -1 && pos < template.length()) {
      list.add(template.substring(pos, nextToken));
      pos = nextToken + TOKEN.length();
      nextToken = template.indexOf(TOKEN, pos);
    }
    list.add(template.substring(pos));
    return list.toArray(new String[list.size()]);
  }

  // fast version of SLF4J MessageFormat.format(), uses {} tokens, no escaping is supported, no array content printing either.
  public static String format(String template, Object... args) {
    String[] templateArr = TEMPLATES.get(template);
    if (templateArr == null) {
      // we may have a race condition here but the end result is idempotent
      templateArr = prepareTemplate(template);
      TEMPLATES.put(template, templateArr);
    }
    StringBuilder sb = new StringBuilder(template.length() * 2);
    for (int i = 0; i < templateArr.length; i++) {
      sb.append(templateArr[i]);
      if (args != null) {
        if (i < templateArr.length - 1) {
          sb.append((i < args.length) ? args[i] : TOKEN);
        }
      }
    }
    return sb.toString();
  }

  //format with lazy-eval
  public static Object formatL(final String template, final Object... args) {
    return new Object() {
      @Override
      public String toString() {
        return format(template, args);
      }
    };
  }

  private static final String PADDING = "000000000000000000000000000000000000";

  public static String intToPaddedString(int value, int padding) {
    StringBuilder sb = new StringBuilder();
    sb.append(value);
    padding = padding - sb.length();
    if (padding > 0) {
      sb.insert(0, PADDING.subSequence(0, padding));
    }
    return sb.toString();
  }

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final String ISO8601_UTC_MASK = "yyyy-MM-dd'T'HH:mm'Z'";

  private static DateFormat getISO8601DateFormat() {
    DateFormat dateFormat = new SimpleDateFormat(ISO8601_UTC_MASK);
    // Stricter parsing to prevent dates such as 2011-12-50T01:00Z (December 50th) from matching
    dateFormat.setLenient(false);
    dateFormat.setTimeZone(UTC);
    return dateFormat;
  }

  public static Date parse(String str) throws ParseException {
    return getISO8601DateFormat().parse(str);
  }

  /**
   * Given an integer, return a string that is in an approximate, but human
   * readable format.
   * It uses the bases 'KiB', 'MiB', and 'GiB' for 1024, 1024**2, and 1024**3.
   * @param number the number to format
   * @return a human readable form of the integer
   */
  public static String humanReadableInt(long number) {
    DecimalFormat oneDecimal = new DecimalFormat("0.0");
    long absNumber = Math.abs(number);
    double result;
    String prefix = number < 0 ? "-" : "";
    String suffix = "";
    if (absNumber < 1000) {
      // since no division has occurred, don't format with a decimal point
      return number + " bytes";
    } else if (absNumber < 1000.0 * 1000.0) {
      result = number / 1000.0;
      suffix = " KB";
    } else if (absNumber < 1000.0 * 1000.0 * 1000.0) {
      result = number / (1000 * 1000);
      suffix = " MB";
    } else {
      result = number / (1000.0 * 1000.0 * 1000.0);
      suffix = " GB";
    }
    return prefix + oneDecimal.format(result) + suffix;
  }

  private static Callable<String> sdcIdCallable;

  public static void setSdcIdCallable(Callable<String> callable) {
    sdcIdCallable = callable;
  }

  public static String getSdcId() {
    Utils.checkState(sdcIdCallable != null, "sdcIdCallable has not been set");
    try {
      return sdcIdCallable.call();
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("SDC ID Callable threw an unexpected exception: {}", ex.toString(), ex));
    }
  }

}
