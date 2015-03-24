/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import java.util.StringTokenizer;

public class Log4jHelper {

  private static final int PERCENT_STATE = 0;
  private static final int LITERAL_STATE = 1;
  private static final int NEXT_LAYOUT = 2;
  private static final int ERROR = -1;

  public static String parseLayout(String patternLayout) {
    int state = NEXT_LAYOUT;
    StringTokenizer stringTokenizer = new StringTokenizer(patternLayout);
    StringBuilder regex = new StringBuilder();
    String argument;
    while (stringTokenizer.hasMoreTokens()) {
      String token = stringTokenizer.nextToken();
      int index = 0;
      StringBuilder partialRegex = new StringBuilder();
      while (index < token.length()) {
        char c = token.charAt(index);
        switch (c) {
          case '%':
            state = PERCENT_STATE;
            break;
          case '-':
            if (state == PERCENT_STATE) {
              //no-op
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append(c);
              state = NEXT_LAYOUT;

            } else if (state == LITERAL_STATE) {
              throw new IllegalArgumentException("hello!!");
            }
            break;
          case '0':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            if (state == PERCENT_STATE) {
              //no-op
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append(c);
              state = NEXT_LAYOUT;
            } else {
              throw new IllegalArgumentException("hello!!");
            }
            break;
          case '.':
            if (state == PERCENT_STATE) {
              //no-op
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append("\\.");
              state = NEXT_LAYOUT;
            } else {
              throw new IllegalArgumentException("hello!!");
            }
            break;

          case '{':
          case '}':
            if (state == NEXT_LAYOUT) {
              partialRegex.append(c);
            } else {
              throw new IllegalArgumentException("hello!!");
            }
            break;

          case 'c' :
            checkStateAndAppend(partialRegex, state, "%{JAVACLASS:logger}", c);
            break;
          case 'C' :
            checkStateAndAppend(partialRegex, state, "%{JAVACLASS:class}", c);
            break;
          case 'F' :
            checkStateAndAppend(partialRegex, state, "%{JAVAFILE:class}", c);
            break;
          case 'l' :
            checkStateAndAppend(partialRegex, state, "%{JAVASTACKTRACEPART:location}", c);
            break;
          case 'L' :
            checkStateAndAppend(partialRegex, state, "%{NONNEGINT:line}", c);
            break;
          case 'm' :
            checkStateAndAppend(partialRegex, state, "%{GREEDYDATA:message}", c);
            break;
          case 'n' :
            checkStateAndAppend(partialRegex, state, "\\r?\\n", c);
            break;
          case 'M' :
            checkStateAndAppend(partialRegex, state, "${WORD:method}", c);
            break;
          case 'p' :
            checkStateAndAppend(partialRegex, state, "%{LOGLEVEL:loglevel}", c);
            break;
          case 'r' :
            checkStateAndAppend(partialRegex, state, "%{INT:relativetime}", c);
            break;
          case 'x' :
            checkStateAndAppend(partialRegex, state, "%{WORD:ndc}?", c);
            break;
          case 'X' :
            argument = getArgument(token, ++index);
            if (null == argument) {
              checkStateAndAppend(partialRegex, state, "\\{(?<mdc>(?:\\{[^\\}]*,[^\\}]*\\})*)\\}", c);
            } else {
              checkStateAndAppend(partialRegex, state, "${WORD:" + argument + "}?", c);
            }
            break;
          case 'd' :
            argument = getArgument(token, ++index);
            checkStateAndAppend(partialRegex, state, getDatePatternFromArgument(argument), c);
            break;
          default:
              partialRegex.append(c);
        }
        index++;
      }
      regex.append(partialRegex.toString()).append(" ");
    }
    return regex.toString().trim();
  }

  private static String getArgument(String token, int index) {
    if (index < token.length()) {
      if (token.charAt(index) == '{') {
        StringBuilder sb = new StringBuilder();
        while (index < token.length() && token.charAt(index) != '}') {
          //read the number of characters
          sb.append(token.charAt(index++));
        }
        return sb.toString();
      }
    }
    return null;
  }

  public static String getDatePatternFromArgument(String dateArgument) {
    String datePattern;
    if(dateArgument == null || dateArgument.isEmpty()) {
      datePattern = getDefaultDatePattern();
    } else {
      switch (dateArgument) {
        case "ISO8601":
          datePattern = "%{TIMESTAMP_ISO8601}";
          break;
        case "ABSOLUTE":
          datePattern = "HH:mm:ss,SSS";
          break;
        default:
          //custom date format, generate regex based on the date format
          datePattern = getPatternFromDateArgument(dateArgument);
      }
    }
    return "(?<date>" + datePattern + ")";
  }

  private static String getMDCRegex() {
    return "";
  }

  public static String getDefaultDatePattern() {
    //no format specified, return default ISO8601 Date Format
    //TIMESTAMP_ISO8601 %{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?
    //YYYY-mm-dd HH:mm:ss,SSS
    return "%{TIMESTAMP_ISO8601}";
  }

  public static String getPatternFromDateArgument(String dateFormat) {
    return "%{TIMESTAMP_ISO8601}";
  }

  private static int checkStateAndAppend(StringBuilder sb, int state, String pattern, char c) {
    if (state == PERCENT_STATE) {
      //encountered conversion character
      sb.append(pattern);
      state = NEXT_LAYOUT;
    } else if (state == NEXT_LAYOUT) {
      sb.append(c);
    } else {
      state = ERROR;
    }
    return state;
  }
}
