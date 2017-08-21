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
import com.streamsets.pipeline.lib.parser.shaded.org.apache.catalina.util.Strftime;

import java.util.Locale;

public class ApacheCustomLogHelper {

  private static final int PERCENT_STATE = 0;
  private static final int LITERAL_STATE = 1;
  private static final int NEXT_LAYOUT = 2;
  private static final int ERROR = -1;

  private ApacheCustomLogHelper() {}

  public static String translateApacheLayoutToGrok(String patternLayout) throws DataParserException {

    int state = NEXT_LAYOUT;
    StringBuilder regex = new StringBuilder();
    StringBuilder partialRegex = new StringBuilder();
    String argument = null;
    int index = 0;
    while (index < patternLayout.length()) {
      char c = patternLayout.charAt(index);
      switch (c) {
        case ' ':
          regex.append(partialRegex.toString()).append(" ");
          partialRegex.setLength(0);
          break;
        case '%':
          state = PERCENT_STATE;
          //read the argument string
          index++;
          StringBuilder sb = new StringBuilder();
          if (index < patternLayout.length()) {
            if (patternLayout.charAt(index) == '{') {
              index++;
              while (index < patternLayout.length() && patternLayout.charAt(index) != '}') {
                //read the number of characters
                sb.append(patternLayout.charAt(index++));
              }
              argument = sb.toString();
            } else if (patternLayout.charAt(index) == '>' || patternLayout.charAt(index) == '<') {
              sb.append(patternLayout.charAt(index));
              argument = sb.toString();
            }
          }
          if(argument == null) {
            index--;
          }
          break;

        case '-':
          if (state == PERCENT_STATE) {
            //FIXME confirm that this is not allowed and throw exception
            throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
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

        case '!':
        case ',':
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
            //ignore
          } else if (state == NEXT_LAYOUT) {
            partialRegex.append(c);
          } else {
            throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
          }
          break;

        case '.':
          if (state == PERCENT_STATE) {
            //FIXME confirm that this is not allowed and throw exception
            throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
          } else if (state == NEXT_LAYOUT) {
            partialRegex.append("\\.");
          } else {
            throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
          }
          break;

        case 'a':
          state = checkStateAndAppend(partialRegex, state, "%{IP:" + ApacheAccessLogConstants.REMOTE_IP_ADDRESS + "}", c);
          break;
        case 'A':
          state = checkStateAndAppend(partialRegex, state, "%{IP:" + ApacheAccessLogConstants.LOCAL_IP_ADDRESS + "}", c);
          break;
        case 'B':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.BYTES_SENT + "}", c);
          break;
        case 'b':
          state = checkStateAndAppend(partialRegex, state, "(?:%{NUMBER:" + ApacheAccessLogConstants.BYTES_SENT + "}|-)", c);
          break;
        case 'C':
          //ignore the name of the cookie from the format string
          state = checkStateAndAppend(partialRegex, state, "%{DATA:cookieContent}", c);
          //reset argument after consuming or in this case after ignoring
          argument = null;
          break;
        case 'D':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" +
            ApacheAccessLogConstants.TIME_TO_SERVE_MICROSECONDS + "}", c);
          break;
        case 'e':
          //ignore argument
          state = checkStateAndAppend(partialRegex, state, "%{DATA:envContent}", c);
          //reset argument after consuming or in this case after ignoring
          argument = null;
          break;
        case 'f':
          state = checkStateAndAppend(partialRegex, state, "%{JAVAFILE:" + ApacheAccessLogConstants.FILENAME + "}", c);
          break;
        case 'h':
          state = checkStateAndAppend(partialRegex, state, "%{IPORHOST:" + ApacheAccessLogConstants.REMOTE_HOST + "}", c);
          break;
        case 'H':
          state = checkStateAndAppend(partialRegex, state, "HTTP/%{NUMBER:" + ApacheAccessLogConstants.HTTP_VERSION + "}",
            c);
          break;
        case 'i':
          //use argument
          /*if(argument != null && argument.equals("Referer")) {
            //(?<requestTime>" + datePattern + ")"
            state = checkStateAndAppend(partialRegex, state, "(?<" + ApacheAccessLogHelper.REFERER + ">[^\"]+)",
              c);
          } else if (argument != null && argument.equals("User-agent")) {
            state = checkStateAndAppend(partialRegex, state, "(?<" + ApacheAccessLogHelper.USER_AGENT + ">[^\"]+)",
              c);
          } else {
            state = checkStateAndAppend(partialRegex, state, "%{DATA:" + "headerContent" + "}",
              c);
          }*/
          if(argument != null && argument.equals("Referer")) {
            //(?<requestTime>" + datePattern + ")"
            state = checkStateAndAppend(partialRegex, state, "%{DATA:" + ApacheAccessLogConstants.REFERER + "}",
              c);
          } else if (argument != null && argument.equals("User-agent")) {
            state = checkStateAndAppend(partialRegex, state, "%{DATA:" + ApacheAccessLogConstants.USER_AGENT + "}",
              c);
          } else {
            state = checkStateAndAppend(partialRegex, state, "%{DATA:" + "headerContent" + "}",
              c);
          }
          //reset argument after consuming
          argument = null;
          break;
        case 'k':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.KEEP_ALIVE + "}", c);
          break;
        case 'l':
          state = checkStateAndAppend(partialRegex, state, "%{USER:" + ApacheAccessLogConstants.LOG_NAME + "}", c);
          break;
        case 'm':
          state = checkStateAndAppend(partialRegex, state, "%{WORD:" + ApacheAccessLogConstants.REQUEST_METHOD + "}", c);
          break;
        case 'n':
          state = checkStateAndAppend(partialRegex, state, "%{DATA:note}", c);
          //reset argument after consuming or in this case after ignoring
          argument = null;
          break;
        case 'o':
          state = checkStateAndAppend(partialRegex, state, "%{DATA:contents}", c);
          //reset argument after consuming or in this case after ignoring
          argument = null;
          break;
        case 'p':
          //FIXME ignore argument
          if(argument != null) {
            if(!argument.equals("canonical") && !argument.equals("local") && !argument.equals("remote")) {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
          }
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.CANONICAL_PORT + "}", c);
          //reset argument after consuming
          argument = null;
          break;
        case 'P':
          //FIXME ignore argument
          if(argument != null) {
            if(!argument.equals("pid") && !argument.equals("tid") && !argument.equals("hextid")) {
              throw new DataParserException(Errors.LOG_PARSER_02, patternLayout);
            }
          }
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.CHILD_PID + "}", c);
          //reset argument after consuming
          argument = null;
          break;
        case 'q':
          state = checkStateAndAppend(partialRegex, state, "%{DATA:" + ApacheAccessLogConstants.QUERY_STRING + "}", c);
          break;
        case 'r':
          state = checkStateAndAppend(partialRegex, state, "%{DATA:" + ApacheAccessLogConstants.REQUEST + "}", c);
          break;
        case 'R':
          state = checkStateAndAppend(partialRegex, state, "%{DATA:" + ApacheAccessLogConstants.RESPONSE_HANDLER + "}", c);
          break;
        case 's':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.STATUS + "}", c);
          argument = null;
          break;
        case 't':
          state = checkStateAndAppend(partialRegex, state, getDatePatternFromArgument(argument), c);
          //reset argument after consuming
          argument = null;
          break;
        case 'T':
          state = checkStateAndAppend(partialRegex, state,
            "%{NUMBER:" + ApacheAccessLogConstants.TIME_TO_SERVE_REQUEST + "}", c);
          break;
        case 'u':
          state = checkStateAndAppend(partialRegex, state, "%{USER:" + ApacheAccessLogConstants.REMOTE_USER + "}", c);
          break;
        case 'U':
          state = checkStateAndAppend(partialRegex, state, "%{NOTSPACE:" + ApacheAccessLogConstants.URL_PATH + "}", c);
          break;
        case 'v':
          state = checkStateAndAppend(partialRegex, state,
            "%{HOST:" + ApacheAccessLogConstants.CANONICAL_SERVER_NAME + "}", c);
          break;
        case 'V':
          state = checkStateAndAppend(partialRegex, state, "%{HOST:" + ApacheAccessLogConstants.SERVER_NAME + "}", c);
          break;
        case 'X':
          state = checkStateAndAppend(partialRegex, state, "%{WORD:" + ApacheAccessLogConstants.CONNECTION_STATUS + "}", c);
          break;
        case 'I':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.BYTES_RECEIVED + "}", c);
          break;
        case 'O':
          state = checkStateAndAppend(partialRegex, state, "%{NUMBER:" + ApacheAccessLogConstants.BYTES_SENT + "}", c);
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

  private static String getArgument(String token, int index) {
    StringBuilder sb = new StringBuilder();
    if (index < token.length()) {
      if (token.charAt(index) == '{') {
        index++;
        while (index < token.length() && token.charAt(index) != '}') {
          //read the number of characters
          sb.append(token.charAt(index++));
        }
        return sb.toString();
      } else if (token.charAt(index) == '>' || token.charAt(index) == '<') {
        sb.append(token.charAt(index));
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
      //convert the strftime(3) date layout to simple date format
      Strftime strftime = new Strftime(dateArgument, Locale.getDefault());
      String simpleDateFormat = strftime.convertDateFormat(dateArgument);
      datePattern = Log4jHelper.getPatternFromDateArgument(simpleDateFormat);
    }
    return "(?<requestTime>" + datePattern + ")";
  }

  public static String getDefaultDatePattern() {
    return "%{HTTPDATE}";
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
