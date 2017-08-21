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
package com.streamsets.datacollector.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

/**
 * Java and it's HttpUrlConnection only supports command line properties for setting proxy hostname and port, but
 * does not expose way to configure username and password. We implement custom Authenticator that add support
 * for additional properties that can be used to tweak username and password.
 */
public class UserPasswordAuthenticator extends Authenticator {

  Logger LOG = LoggerFactory.getLogger(UserPasswordAuthenticator.class);

  private static class ProxyInfo {
    String hostname;
    String port;
    String username;
    String password;

    public boolean isEmpty() {
      return hostname.isEmpty() && port.isEmpty() && username.isEmpty() && password.isEmpty();
    }

    public boolean isFor(String hostname, int port) {
      return this.hostname.equalsIgnoreCase(hostname) && port == Integer.parseInt(this.port);
    }
  }

  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    LOG.trace("Get password request for type {}", getRequestorType());

    if (getRequestorType() == RequestorType.PROXY) {
      String protocol = getRequestingProtocol().toLowerCase();
      ProxyInfo proxyInfo = retrieveInfoForProtocol(protocol);

      // Java can call this method with protocol http even though that the target URL is https. Hence if
      // this is http protocol and the proxy info is empty, we'll look up proxy for protocol https.
      if("http".equals(protocol) && proxyInfo.isEmpty()) {
        LOG.trace("Discarding missing proxy info for protocol http, loading https instead");
        proxyInfo = retrieveInfoForProtocol("https");
      }

      // Send the credentials only to configured proxy and not to a different host
      if(proxyInfo.isFor(getRequestingHost(), getRequestingPort())) {
        return new PasswordAuthentication(proxyInfo.username, proxyInfo.password.toCharArray());
      }
    }

    LOG.trace("Not returning any authentication info");
    return null;
  }

  private ProxyInfo retrieveInfoForProtocol(String protocol) {
    ProxyInfo info = new ProxyInfo();

    info.hostname = System.getProperty(protocol + ".proxyHost", "");
    info.port     = System.getProperty(protocol + ".proxyPort", "");
    info.username = System.getProperty(protocol + ".proxyUser", "");
    info.password = System.getProperty(protocol + ".proxyPassword", "");

    LOG.trace("Retrieved proxy for protocol {}: hostname({}), port({})", protocol, info.hostname, info.port);
    return info;
  }
}
