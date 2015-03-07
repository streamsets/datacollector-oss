/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringEL {

  @ElFunction(
    prefix = "str",
    name = "substring",
    description = "Returns a new string that is a substring of this string. " +
    "The substring begins at the specified beginIndex and extends to the character at index endIndex-1. " +
    "Thus the length of the substring is endIndex-beginIndex")
  public static String substring(
    @ElParam("string") String string,
    @ElParam("beginIndex") int beginIndex,
    @ElParam("endIndex") int endIndex) {
    Utils.checkArgument(beginIndex >= 0, "Argument beginIndex should be 0 or greater");
    Utils.checkArgument(endIndex >= 0, "Argument endIndex should be 0 or greater");

    if(string == null || string.isEmpty()) {
      return string;
    }
    int length = string.length();
    if(beginIndex > length) {
      return "";
    }
    if(endIndex > string.length()) {
      endIndex = string.length();
    }
    return string.substring(beginIndex, endIndex);
  }

  @ElFunction(
    prefix = "str",
    name = "trim",
    description = "Removes leading and trailing whitespaces")
  public static String trim(
    @ElParam("string") String string) {
    return string.trim();
  }

  @ElFunction(
    prefix = "str",
    name = "toUpper",
    description = "Converts all of the characters in the argument string to uppercase")
  public static String toUpper(
    @ElParam("string") String string) {
    return string.toUpperCase();
  }

  @ElFunction(
    prefix = "str",
    name = "toLower",
    description = "Converts all of the characters in the argument string to lowercase")
  public static String toLower(
    @ElParam("string") String string) {
    return string.toLowerCase();
  }

  @ElFunction(
    prefix = "str",
    name = "replace",
    description = "Returns a new string resulting from replacing all occurrences of oldChar in this string with newChar")
  public static String replace(
    @ElParam("string") String string,
    @ElParam("oldChar") char oldChar,
    @ElParam("newChar") char newChar) {
    return string.replace(oldChar, newChar);
  }

  @ElFunction(
    prefix = "str",
    name = "replaceAll",
    description = "Replaces each substring of this string that matches the given regEx with the given replacement")
  public static String replaceAll(
    @ElParam("string") String string,
    @ElParam("regEx") String regEx,
    @ElParam("replacement") String replacement) {
    return string.replaceAll(regEx, replacement);
  }

  @ElFunction(
    prefix = "str",
    name = "truncate",
    description = "Truncates the argument string to the given index")
  public static String truncate(
    @ElParam("string") String string,
    @ElParam("endIndex") int endIndex) {
    return string.substring(0, endIndex);
  }

  @ElFunction(
    prefix = "str",
    name = "regExCapture",
    description = "Captures the string that matches the argument regular expression and the group")
  public static String regExCapture(
    @ElParam("string") String string,
    @ElParam("regEx") String regEx,
    @ElParam("groupNumber") int groupNumber) {
    Pattern pattern = Pattern.compile(regEx);
    Matcher matcher = pattern.matcher(string);
    if(matcher.find()) {
      return matcher.group(groupNumber);
    }
    return null;
  }

  @ElFunction(
    prefix = "str",
    name = "contains",
    description = "Indicates whether argument string s1 contains argument string s2")
  public static boolean contains(
    @ElParam("s1") String s1,
    @ElParam("s1") String s2) {
    return s1.contains(s2);
  }

  @ElFunction(
    prefix = "str",
    name = "startsWith",
    description = "Indicates whether argument string s1 starts with argument string s2")
  public static boolean startsWith(
    @ElParam("s1") String s1,
    @ElParam("s1") String s2) {
    return s1.startsWith(s2);
  }

  @ElFunction(
    prefix = "str",
    name = "endsWith",
    description = "Indicates whether argument string s1 ends with argument string s2")
  public static boolean endsWith(
    @ElParam("s1") String s1,
    @ElParam("s1") String s2) {
    return s1.endsWith(s2);
  }
}
