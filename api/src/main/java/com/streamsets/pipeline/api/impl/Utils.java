/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.slf4j.helpers.MessageFormatter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class Utils {

  Utils() {
  }

  public static <T> T checkNotNull(T value, String varName) {
    if (value == null) {
      throw new NullPointerException(format("{} cannot be null", varName));
    }
    return value;
  }

  public static void checkArgument(boolean expression, String msg) {
    if (!expression) {
      throw new IllegalArgumentException(msg);
    }
  }

  public static void checkState(boolean expression, String msg) {
    if (!expression) {
      throw new IllegalStateException(msg);
    }
  }

  public static String format(String template, Object... args) {
    return MessageFormatter.arrayFormat(template, args).getMessage();
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

}
