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

package com.streamsets.pipeline.lib.websocket;

import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.internal.util.Base64;

public class WebSocketCommon {

  public static WebSocketClient createWebSocketClient(String resourceUrl, TlsConfigBean tlsConf) {
    try {
      resourceUrl = resourceUrl.toLowerCase();
      if (resourceUrl.startsWith("wss")) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        if (tlsConf != null && tlsConf.isEnabled() && tlsConf.isInitialized()) {
          if (tlsConf.keyStoreFilePath != null) {
            sslContextFactory.setKeyStorePath(tlsConf.keyStoreFilePath);
          }
          if (tlsConf.keyStoreType != null) {
            sslContextFactory.setKeyStoreType(tlsConf.keyStoreType.getJavaValue());
          }
          if (tlsConf.keyStorePassword != null) {
            sslContextFactory.setKeyStorePassword(tlsConf.keyStorePassword.get());
          }
          if (tlsConf.trustStoreFilePath != null) {
            sslContextFactory.setTrustStorePath(tlsConf.trustStoreFilePath);
          }
          if (tlsConf.trustStoreType != null) {
            sslContextFactory.setTrustStoreType(tlsConf.trustStoreType.getJavaValue());
          }
          if (tlsConf.trustStorePassword != null) {
            sslContextFactory.setTrustStorePassword(tlsConf.trustStorePassword.get());
          }

          sslContextFactory.setSslContext(tlsConf.getSslContext());
          sslContextFactory.setIncludeCipherSuites(tlsConf.getFinalCipherSuites());
          sslContextFactory.setIncludeProtocols(tlsConf.getFinalProtocols());
        }
        return new WebSocketClient(sslContextFactory);
      } else {
       return new WebSocketClient();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(resourceUrl, e);
    }
  }

  public static String generateBasicAuthHeader(String username, String password) {
    if (username == null) {
      username = "";
    }

    if (password == null) {
      password = "";
    }

    final byte[] prefix = (username + ":").getBytes();
    final byte[] passwordByte = password.getBytes();
    final byte[] usernamePassword = new byte[prefix.length + passwordByte.length];

    System.arraycopy(prefix, 0, usernamePassword, 0, prefix.length);
    System.arraycopy(passwordByte, 0, usernamePassword, prefix.length, passwordByte.length);

    return "Basic " + Base64.encodeAsString(usernamePassword);
  }
}
