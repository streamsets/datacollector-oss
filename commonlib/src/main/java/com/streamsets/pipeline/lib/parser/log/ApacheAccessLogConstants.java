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

public class ApacheAccessLogConstants {

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
  static final String HTTP_VERSION = "httpversion";

  private ApacheAccessLogConstants() {}
}
