/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

public class BasicConnectionString {
  private static final String HOSTNAME_REGEX = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-\\_]*[a-zA-Z0-9])\\.)*" +
      "([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])*";

  private static final String IP_ADDRESS_V4_REGEX = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}" +
      "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])*";

  private static final String IP_ADDRESS_V6_REGEX = "((((?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4})))";

  private static final String
      IP_ADDRESS_V6_HEX_COMPRESSED_REGEX
      = "(((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)*)";

  /**
   * Bear in mind that all the REGEX included here must have the same exact number of groups since they are used in
   * all the JDBC branded implementations.
   *
   * @return List of all the regex included
   */
  public static List<String> getAllRegex() {
    return Arrays.asList(HOSTNAME_REGEX, IP_ADDRESS_V4_REGEX, IP_ADDRESS_V6_REGEX, IP_ADDRESS_V6_HEX_COMPRESSED_REGEX);
  }

  /**
   * Bean containing the basic information from connection string: host, port and tail.
   */
  public static class Info {
    private final String host;
    private final int port;
    private final String tail;

    private Info(String host, int port, String tail) {
      this.host = host;
      this.port = port;
      this.tail = tail;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public String getTail() {
      return tail;
    }

    public Info changeHostPort(String host, int port) {
      return new Info(host, port, getTail());
    }

  }

  /**
   * Basic connection pattern finder and data extractor.
   */
  public static class Pattern {
    private final java.util.regex.Pattern pattern;
    private final int hostGroup;
    private final int portGroup;
    private final String defaultHost;
    private final int defaultPort;
    private final int tailGroup;

    public Pattern(
        String regex, int hostGroup, String defaultHost, int portGroup, int defaultPort, int tailGroup
    ) {
      this.pattern = java.util.regex.Pattern.compile(regex);
      this.hostGroup = hostGroup;
      this.defaultHost = defaultHost;
      this.portGroup = portGroup;
      this.defaultPort = defaultPort;
      this.tailGroup = tailGroup;
    }

    protected Info find(String connectionString) {
      String host;
      int port;
      String tail = "";
      Matcher matcher = pattern.matcher(connectionString);

      if (matcher.matches()) {
        if (hostGroup > 0) {
          host = matcher.group(hostGroup);
        } else {
          host = defaultHost;
        }
        if (portGroup > 0) {
          port = Integer.parseInt(matcher.group(portGroup));
        } else {
          port = defaultPort;
        }
        if (tailGroup > 0) {
          tail = matcher.group(tailGroup);
          if (tail == null){
            tail = "";
          }
        }
        return new Info(host, port, tail);
      } else {
        return null;
      }
    }
  }

  private final Set<Pattern> patterns;
  private final String basicConnectionStringTemplate;

  public BasicConnectionString(
      Set<Pattern> patterns, String basicConnectionStringTemplate
  ) {
    this.patterns = Utils.checkNotNull(patterns, "basicConnectionPatterns");
    this.basicConnectionStringTemplate = Utils.checkNotNull(basicConnectionStringTemplate,
        "basicConnectionStringTemplate"
    );
  }

  public Info getBasicConnectionInfo(String connectionString) {
    for (Pattern pattern : patterns) {
      Info info = pattern.find(connectionString);
      if (info != null) {
        return info;
      }
    }
    return null;
  }

  public String getBasicConnectionUrl(Info info) {
    Utils.checkNotNull(info, "info");
    return String.format(basicConnectionStringTemplate, info.getHost(), info.getPort(), info.getTail());
  }
}
