/*
 * Copyright 2021 StreamSets Inc.
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

import javax.servlet.http.HttpServletRequest;

/**
 * Exception thrown by SSOService validation when a component ID moved to a different system.
 */
public class MovedException extends RuntimeException {
  private final String url;

  public MovedException(String url) {
    super("Location: " + url);
    this.url = url;
  }

  /**
   * Returns the MOVED PERMANENTLY location URL.
   */
  public String getUrl() {
    return url;
  }

  /**
   * Returns the base URL of the MOVED PERMANENTLY location URL.
   */
  public String getBaseUrl() {
    return extractBaseUrl(url);
  }

  public String getNewUrl(HttpServletRequest req) {
    StringBuilder sb = new StringBuilder(getBaseUrl()).append(req.getRequestURI());
    if (req.getQueryString() != null) {
      sb.append("?").append(req.getQueryString());
    }
    return sb.toString();
  }

  /**
   * Extracts the base URL (SCHEMA://HOST[:PORT]) of the given URL.
   */
  public static String extractBaseUrl(String url) {
    String baseUrl = null;
    int hostStart = url.indexOf("//");
    if (hostStart > -1) {
      hostStart += 2;
      int pathStart = url.indexOf("/", hostStart);
      if (pathStart == -1) {
        baseUrl = url;
      } else {
        baseUrl = url.substring(0, pathStart);
      }
    }
    return baseUrl;
  }

}
