/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Label;

public enum LogMode implements Label {

  /*
    Example - 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
    A "-" in a field indicates missing data.
	  •	127.0.0.1 is the IP address of the client (remote host) which made the request to the server.
	  •	user-identifier is the RFC 1413 identity of the client.
	  •	frank is the userid of the person requesting the document.
	  •	[10/Oct/2000:13:55:36 -0700] is the date, time, and time zone when the server finished processing the request,
	    by default in strftime format %d/%b/%Y:%H:%M:%S %z.
	  •	"GET /apache_pb.gif HTTP/1.0" is the request line from the client. The method GET, /apache_pb.gif the resource
	    requested, and HTTP/1.0 the HTTP protocol.
	  •	200 is the HTTP status code returned to the client. 2xx is a successful response, 3xx a redirection, 4xx a client
	  error, and 5xx a server error.
	  •	2326 is the size of the object returned to the client, measured in bytes.
  */
  COMMON_LOG_FORMAT("Common Log Format"),

  ;

  private final String label;

  LogMode(String label) {
    this.label = label;

  }

  @Override
  public String getLabel() {
    return label;
  }

}
