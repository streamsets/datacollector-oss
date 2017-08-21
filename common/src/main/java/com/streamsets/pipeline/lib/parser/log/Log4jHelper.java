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

import com.streamsets.pipeline.lib.parser.DataParserException;

public class Log4jHelper {

  private static final int PERCENT_STATE = 0;
  private static final int LITERAL_STATE = 1;
  private static final int NEXT_LAYOUT = 2;
  private static final int ERROR = -1;
  private static final String TRAILING_SPACE = "(?:\\s*)";

  private Log4jHelper() {}

  public static String translateLog4jLayoutToGrok(String patternLayout) throws DataParserException {
    //remove trailing '%n's since the reader does not return the new line character
    while(patternLayout.endsWith("%n")) {
      patternLayout = patternLayout.substring(0, patternLayout.length()-2);
    }

    int state = NEXT_LAYOUT;
    StringBuilder regex = new StringBuilder();
    StringBuilder partialRegex = new StringBuilder();
    String argument;
    int index = 0;
    boolean leftPadWithSpace = false;
    boolean rightPadWithSpace = false;
    while (index < patternLayout.length()) {

        char c = patternLayout.charAt(index);
        switch (c) {
          case ' ':
            regex.append(partialRegex.toString()).append(" ");
            partialRegex.setLength(0);
            leftPadWithSpace = false;
            rightPadWithSpace = false;
            break;
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
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            } else if (state == NEXT_LAYOUT) {
              partialRegex.append("\\").append(c);
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
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVACLASS:category}", c, rightPadWithSpace);
            break;
          case 'C' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVACLASS:class}", c, rightPadWithSpace);
            break;
          case 'F' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVAFILE:filename}", c, rightPadWithSpace);
            break;
          case 'l' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{JAVASTACKTRACEPART:location}", c, rightPadWithSpace);
            break;
          case 'L' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{NONNEGINT:line}", c, rightPadWithSpace);
            break;
          case 'm' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{GREEDYDATA:message}", c, rightPadWithSpace);
            break;
          case 'n' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "\\r?\\n", c, rightPadWithSpace);
            break;
          case 'M' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{WORD:method}", c, rightPadWithSpace);
            break;
          case 'p' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{LOGLEVEL:severity}", c, rightPadWithSpace);
            break;
          case 'r' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{INT:relativetime}", c, rightPadWithSpace);
            break;
          case 't' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{DATA:thread}", c, rightPadWithSpace);
            break;
          case 'x' :
            index += ignoreArgument(patternLayout, index);
            state = checkStateAndAppend(partialRegex, state, "%{DATA:ndc}?", c, rightPadWithSpace);
            break;
          case 'X' :
            argument = getArgument(patternLayout, ++index);
            if (null == argument) {
              state = checkStateAndAppend(partialRegex, state, getDefaultMDCPattern(), c, rightPadWithSpace);
            } else {
              index += argument.length() + 1 /* +1 for '}'*/;
              state = checkStateAndAppend(partialRegex, state, "%{DATA:" + argument + "}?", c, rightPadWithSpace);
            }
            break;
          case 'd' :
            argument = getArgument(patternLayout, ++index);
            if(argument != null) {
              index += argument.length() + 1 /* +1 for the '}'*/;
            } else {
              index--;
            }
            state = checkStateAndAppend(partialRegex, state, getDatePatternFromArgument(argument), c, rightPadWithSpace);
            break;
          default:
            if(state == PERCENT_STATE) {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
            partialRegex.append(c);
        }
        index++;
    }
    regex.append(partialRegex.toString());
    return "^" /*begins with*/ + regex.toString().trim();
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
          datePattern = "%{HOUR}:%{MINUTE}:%{SECOND}";
          break;
        case "DATE" :
          datePattern = "%{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}:%{SECOND}";
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
