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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

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
      if (i < templateArr.length - 1) {
        sb.append((i < args.length) ? args[i] : TOKEN);
      }
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

}
