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
package com.streamsets.lib.security.http;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

public class SSOPrincipalUtils {
  private static final Splitter xffSplitter = Splitter.on(",").omitEmptyStrings().trimResults();

  static final String CLIENT_IP_HEADER = "X-Forwarded-For";
  static final String UNKNOWN_IP = "unknown";

  public static String getClientIpAddress(HttpServletRequest request) {
    String xff = request.getHeader(CLIENT_IP_HEADER);

    List<String> ipList = xffSplitter.splitToList(Strings.nullToEmpty(xff));

    String ip;
    if (ipList.size() == 0 || UNKNOWN_IP.equalsIgnoreCase(ipList.get(0))) {
      ip = request.getRemoteAddr();
      if (Strings.isNullOrEmpty(ip)) {
        ip = UNKNOWN_IP;
      }
    } else {
      ip = ipList.get(0);
    }
    return ip;
  }

  public static void setRequestInfo(SSOPrincipal principal, ServletRequest request) {
    ((SSOPrincipalJson) principal).setRequestIpAddress(getClientIpAddress((HttpServletRequest)request));
  }

}
