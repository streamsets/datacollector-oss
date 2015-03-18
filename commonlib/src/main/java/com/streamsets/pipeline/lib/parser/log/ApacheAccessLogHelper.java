/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.util.HashMap;
import java.util.Map;

public class ApacheAccessLogHelper {

  //TODO Support the following formats if required
  //%{c}a	Underlying peer IP address of the connection
  //%{varname}C
  //%{VARNAME}e	The contents of the environment variable VARNAME.
  //%{VARNAME}i	The contents of VARNAME: header line(s) in the request sent to the server.
  //%{VARNAME}n	The contents of note VARNAME from another module.
  //%{VARNAME}o	The contents of VARNAME: header line(s) in the reply.
  //%{format}p	The canonical port of the server serving the request, or the server's actual port,
  //%{format}P	The process ID or thread ID of the child that serviced the request.
  //%{format}t
  //%{VARNAME}^ti	The contents of VARNAME: trailer line(s) in the request sent to the server.
  //%{VARNAME}^to	The contents of VARNAME: trailer line(s) in the response sent from the server.
  //%L

  /*Supported formats*/
  static final String LOG_FORMAT_PERCENTAGE = "%%";
  static final String LOG_FORMAT_REMOTE_IP_ADDRESS = "%a";
  static final String LOG_FORMAT_LOCAL_IP_ADDRESS = "%A";
  static final String LOG_FORMAT_RESPONSE_SIZE = "%b";
  static final String LOG_FORMAT_RESPONSE_SIZE_CLF = "%B";
  static final String LOG_FORMAT_TIME_TO_SERVE_MICRO_SECONDS = "%D";
  static final String LOG_FORMAT_FILE_NAME = "%f";
  static final String LOG_FORMAT_REMOTE_HOST = "%h";
  static final String LOG_FORMAT_REQUEST_PROTOCOL = "%H";
  static final String LOG_FORMAT_NUMBER_OF_KEEPALIVE_REQUESTS = "%k";
  static final String LOG_FORMAT_REMOTE_LOGNAME = "%l";
  static final String LOG_FORMAT_REQUEST_METHOD = "%m";
  static final String LOG_FORMAT_SERVER_PORT = "%p";
  static final String LOG_FORMAT_CHILD_PID = "%P";
  static final String LOG_FORMAT_QUERY_STRING = "%q";
  static final String LOG_FORMAT_REQUEST = "%r";
  static final String LOG_FORMAT_HANDLER_GENERATING_REQUEST = "%R";
  static final String LOG_FORMAT_STATUS = "%s";
  static final String LOG_FORMAT_FINAL_REQUEST_STATUS = "%>s";
  static final String LOG_FORMAT_REQUEST_RECEIVED_TIME="%t";
  static final String LOG_FORMAT_TIME_TO_SERVE_REQUEST="%T";
  static final String LOG_FORMAT_REMOTE_USER = "%u";
  static final String LOG_FORMAT_URL_PATH = "%U";
  static final String LOG_FORMAT_CANONICAL_SERVER_NAME = "%v";
  static final String LOG_FORMAT_SERVER_NAME = "%V";
  static final String LOG_FORMAT_CONNECTION_STATUS = "%X";
  static final String LOG_FORMAT_BYTES_RECEIVED = "%I";
  static final String LOG_FORMAT_BYTES_SENT = "%I";
  static final String LOG_FORMAT_BYTES_TRANSFERRED = "%S";
  static final String LOG_FORMAT_REFERER = "%{Referer}i";
  static final String LOG_FORMAT_USER_AGENT = "%{User-agent}i";

  //Supported regex groups
  private static final String REG_EX_STRING = "(\\S+)";
  private static final String REG_EX_REQUEST_LINE = "(\\S+ \\S+ \\S+)";
  private static final String REG_EX_REQUEST_TIME = "\\[([\\w:/]+\\s[+\\-]\\d{4})\\]";
  private static final String REG_EX_STATUS = "(\\d{3})";
  private static final String REG_EX_NUMBER = "(\\d+)";
  private static final String REG_EX_URL = "([^\"]*)";
  private static final String REG_EX_PERCENTAGE = "%";

  //Field names for supported formats
  static final String REMOTE_IP_ADDRESS = "remoteIpAddress";
  static final String LOCAL_IP_ADDRESS = "localIpAddress";
  static final String REQUEST_TIME = "requestTime";
  static final String REQUEST = "request";
  static final String LOG_NAME = "logName";
  static final String REMOTE_HOST = "remoteHost";
  static final String REMOTE_USER = "remoteUser";
  static final String STATUS = "status";
  static final String BYTES_SENT = "bytesSent";
  static final String REQUEST_METHOD = "requestMethod";
  static final String REQUEST_PROTOCOL = "requestProtocol";
  static final String URL_PATH = "urlPath";
  static final String REFERER = "referer";
  static final String USER_AGENT = "userAgent";
  static final String TIME_TO_SERVE_MICROSECONDS = "timeToServeMicroSecs";
  static final String FILENAME = "filename";
  static final String KEEP_ALIVE = "keepAlive";
  static final String CANONICAL_PORT = "canonicalPort";
  static final String CHILD_PID = "childPid";
  static final String QUERY_STRING = "queryString";
  static final String RESPONSE_HANDLER = "responseHandler";
  static final String TIME_TO_SERVE_REQUEST = "timeToServeRequest";
  static final String CANONICAL_SERVER_NAME = "canonicalServerName";
  static final String SERVER_NAME = "canonicalServerName";
  static final String CONNECTION_STATUS = "connectionStatus";
  static final String BYTES_RECEIVED = "bytesReceived";
  static final String BYTES_TRANSFERRED = "bytesTransferred";

  public static Map<String, String> formatToRegExMap = new HashMap<>();
  public static Map<String, String> formatToFieldNameMap = new HashMap<>();

  static {
    formatToRegExMap.put(LOG_FORMAT_REMOTE_IP_ADDRESS, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_LOCAL_IP_ADDRESS, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_REQUEST_RECEIVED_TIME, REG_EX_REQUEST_TIME);
    formatToRegExMap.put(LOG_FORMAT_REQUEST, REG_EX_REQUEST_LINE);
    formatToRegExMap.put(LOG_FORMAT_REMOTE_LOGNAME, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_REMOTE_HOST, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_REMOTE_USER, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_FINAL_REQUEST_STATUS, REG_EX_STATUS);
    formatToRegExMap.put(LOG_FORMAT_RESPONSE_SIZE, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_RESPONSE_SIZE_CLF, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_REQUEST_METHOD, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_REQUEST_PROTOCOL, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_URL_PATH, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_REFERER, REG_EX_URL);
    formatToRegExMap.put(LOG_FORMAT_USER_AGENT, REG_EX_URL);
    formatToRegExMap.put(LOG_FORMAT_TIME_TO_SERVE_MICRO_SECONDS, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_FILE_NAME, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_NUMBER_OF_KEEPALIVE_REQUESTS, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_SERVER_PORT, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_CHILD_PID, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_QUERY_STRING, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_HANDLER_GENERATING_REQUEST, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_STATUS, REG_EX_STATUS);
    formatToRegExMap.put(LOG_FORMAT_TIME_TO_SERVE_REQUEST, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_CANONICAL_SERVER_NAME, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_SERVER_NAME, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_CONNECTION_STATUS, REG_EX_STRING);
    formatToRegExMap.put(LOG_FORMAT_BYTES_RECEIVED, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_BYTES_SENT, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_BYTES_TRANSFERRED, REG_EX_NUMBER);
    formatToRegExMap.put(LOG_FORMAT_PERCENTAGE, REG_EX_PERCENTAGE);

    formatToFieldNameMap.put(LOG_FORMAT_REMOTE_IP_ADDRESS, REMOTE_IP_ADDRESS);
    formatToFieldNameMap.put(LOG_FORMAT_LOCAL_IP_ADDRESS, LOCAL_IP_ADDRESS);
    formatToFieldNameMap.put(LOG_FORMAT_REQUEST_RECEIVED_TIME, REQUEST_TIME);
    formatToFieldNameMap.put(LOG_FORMAT_REQUEST, REQUEST);
    formatToFieldNameMap.put(LOG_FORMAT_REMOTE_LOGNAME, LOG_NAME);
    formatToFieldNameMap.put(LOG_FORMAT_REMOTE_HOST, REMOTE_HOST);
    formatToFieldNameMap.put(LOG_FORMAT_REMOTE_USER, REMOTE_USER);
    formatToFieldNameMap.put(LOG_FORMAT_FINAL_REQUEST_STATUS, STATUS);
    formatToFieldNameMap.put(LOG_FORMAT_RESPONSE_SIZE, BYTES_SENT);
    formatToFieldNameMap.put(LOG_FORMAT_RESPONSE_SIZE_CLF, BYTES_SENT);
    formatToFieldNameMap.put(LOG_FORMAT_REQUEST_METHOD, REQUEST_METHOD);
    formatToFieldNameMap.put(LOG_FORMAT_REQUEST_PROTOCOL, REQUEST_PROTOCOL);
    formatToFieldNameMap.put(LOG_FORMAT_URL_PATH, URL_PATH);
    formatToFieldNameMap.put(LOG_FORMAT_REFERER, REFERER);
    formatToFieldNameMap.put(LOG_FORMAT_USER_AGENT, USER_AGENT);
    formatToFieldNameMap.put(LOG_FORMAT_TIME_TO_SERVE_MICRO_SECONDS, TIME_TO_SERVE_MICROSECONDS);
    formatToFieldNameMap.put(LOG_FORMAT_FILE_NAME, FILENAME);
    formatToFieldNameMap.put(LOG_FORMAT_NUMBER_OF_KEEPALIVE_REQUESTS, KEEP_ALIVE);
    formatToFieldNameMap.put(LOG_FORMAT_SERVER_PORT, CANONICAL_PORT);
    formatToFieldNameMap.put(LOG_FORMAT_CHILD_PID, CHILD_PID);
    formatToFieldNameMap.put(LOG_FORMAT_QUERY_STRING, QUERY_STRING);
    formatToFieldNameMap.put(LOG_FORMAT_HANDLER_GENERATING_REQUEST, RESPONSE_HANDLER);
    formatToFieldNameMap.put(LOG_FORMAT_STATUS, STATUS);
    formatToFieldNameMap.put(LOG_FORMAT_TIME_TO_SERVE_REQUEST, TIME_TO_SERVE_REQUEST);
    formatToFieldNameMap.put(LOG_FORMAT_CANONICAL_SERVER_NAME, CANONICAL_SERVER_NAME);
    formatToFieldNameMap.put(LOG_FORMAT_SERVER_NAME, SERVER_NAME);
    formatToFieldNameMap.put(LOG_FORMAT_CONNECTION_STATUS, CONNECTION_STATUS);
    formatToFieldNameMap.put(LOG_FORMAT_BYTES_RECEIVED, BYTES_RECEIVED);
    formatToFieldNameMap.put(LOG_FORMAT_BYTES_SENT, BYTES_SENT);
    formatToFieldNameMap.put(LOG_FORMAT_BYTES_TRANSFERRED, BYTES_TRANSFERRED);
  }

  public static String convertLogFormatToRegEx(String logFormat, Map<String, Integer> fieldToGroupMap)
    throws DataParserException {
    Utils.checkNotNull(logFormat, "customLogFormat");
    StringBuilder regEx = new StringBuilder();
    int group = 0;
    int index = 0;
    char c;
    regEx.append("^");
    while(index < logFormat.length()) {
      c = logFormat.charAt(index);
      switch(c) {
        case ' ':
        case '"':
          regEx.append(c);
          break;
        case '%':
          StringBuilder format = new StringBuilder();
          format.append(c);
          index++;
          while(index < logFormat.length()) {
            c  = logFormat.charAt(index);
            if(c != ' ' && c != '"') {
              format.append(c);
              index++;
            } else {
              break;
            }
          }
          String formatString = format.toString();
          if(formatToRegExMap.containsKey(formatString)) {
            regEx.append(formatToRegExMap.get(formatString));
            fieldToGroupMap.put(formatToFieldNameMap.get(formatString), ++group);
          } else {
            throw new DataParserException(Errors.LOG_PARSER_02, formatString);
          }
          //append the last character if it was ' '
          if(index < logFormat.length()) {
            regEx.append(logFormat.charAt(index));
          }
          break;
        default :
          regEx.append(c);
      }
      index++;
    }
    return regEx.toString();
  }

}
