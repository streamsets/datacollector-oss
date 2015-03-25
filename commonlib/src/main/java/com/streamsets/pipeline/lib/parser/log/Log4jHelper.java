/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.lib.parser.DataParserException;

import java.util.StringTokenizer;

public class Log4jHelper {

  private static final int PERCENT_STATE = 0;
  private static final int LITERAL_STATE = 1;
  private static final int NEXT_LAYOUT = 2;
  private static final int ERROR = -1;
  private static final String TRAILING_SPACE = "(?:\\s*)";

  public static String translateLog4jLayoutToGrok(String patternLayout) throws DataParserException {
    int state = NEXT_LAYOUT;
    StringTokenizer stringTokenizer = new StringTokenizer(patternLayout);
    StringBuilder regex = new StringBuilder();
    String argument;
    while (stringTokenizer.hasMoreTokens()) {
      boolean leftPadWithSpace = false;
      boolean rightPadWithSpace = false;
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
              rightPadWithSpace = true;
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append(c);
            } else if (state == LITERAL_STATE) {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
            break;
          case '[':
          case ']':
            if (state == PERCENT_STATE) {
            } else if (state == NEXT_LAYOUT) {
              //partialRegex.append("\\\\").append(c);
            } else if (state == LITERAL_STATE) {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
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
              if(!rightPadWithSpace && !leftPadWithSpace) {
                partialRegex.append(TRAILING_SPACE);
                leftPadWithSpace = true;
              }
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append(c);
            } else {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
            break;
          case '.':
            if (state == PERCENT_STATE) {
              //no-op
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append("\\.");
            } else {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
            break;

          case 'c' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVACLASS:category}", c, rightPadWithSpace);
            break;
          case 'C' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVACLASS:class}", c, rightPadWithSpace);
            break;
          case 'F' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVAFILE:class}", c, rightPadWithSpace);
            break;
          case 'l' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVASTACKTRACEPART:location}", c, rightPadWithSpace);
            break;
          case 'L' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{NONNEGINT:line}", c, rightPadWithSpace);
            break;
          case 'm' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{GREEDYDATA:message}", c, rightPadWithSpace);
            break;
          case 'n' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "\\r?\\n", c, rightPadWithSpace);
            break;
          case 'M' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "${WORD:method}", c, rightPadWithSpace);
            break;
          case 'p' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{LOGLEVEL:severity}", c, rightPadWithSpace);
            break;
          case 'r' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{INT:relativetime}", c, rightPadWithSpace);
            break;
          case 't' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{PROG:thread}", c, rightPadWithSpace);
            break;
          case 'x' :
            index += ignoreArgument(token, index);
            state = checkStateAndAppend(partialRegex, state, "%{WORD:ndc}?", c, rightPadWithSpace);
            break;
          case 'X' :
            argument = getArgument(token, ++index);
            if (null == argument) {
              state = checkStateAndAppend(partialRegex, state, getDefaultMDCPattern(), c, rightPadWithSpace);
            } else {
              index += argument.length() + 1 /* +1 for '}'*/;
              state = checkStateAndAppend(partialRegex, state, "${WORD:" + argument + "}?", c, rightPadWithSpace);
            }
            break;
          case 'd' :
            argument = getArgument(token, ++index);
            if(argument != null) {
              index += argument.length() + 1 /* +1 for the '}'*/;
            }
            state = checkStateAndAppend(partialRegex, state, getDatePatternFromArgument(argument), c, rightPadWithSpace);
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

  private static int ignoreArgument(String token, int index) {
    String argument = getArgument(token, ++index);
    if(argument != null) {
      return argument.length() + 2 /*+1 for '{' and +1 for '}'*/;
    }
    return 0;
  }

  private static String getArgument(String token, int index) {
    if (index < token.length()) {
      if (token.charAt(index) == '{') {
        index++;
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
    return "(?<timestamp>" + datePattern + ")";
  }

  private static String getDefaultMDCPattern() {
    return "\\{(?<mdc>(?:\\{[^\\}]*,[^\\}]*\\})*)\\}";
  }

  public static String getDefaultDatePattern() {
    return "%{TIMESTAMP_ISO8601}";
  }

  public static String getPatternFromDateArgument(String dateFormat) {
    return ".{" + dateFormat.length() + "}";
  }

  private static int checkStateAndAppend(StringBuilder sb, int state, String pattern, char c,
                                         boolean rightPadWithSpace) {
    if (state == PERCENT_STATE) {
      //encountered conversion character
      sb.append(pattern);
      if(rightPadWithSpace) {
        sb.append(TRAILING_SPACE);
      }
      state = NEXT_LAYOUT;
    } else if (state == NEXT_LAYOUT) {
      sb.append(c);
    } else {
      state = ERROR;
    }
    return state;
  }
}
