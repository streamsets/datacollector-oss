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
package com.streamsets.datacollector.vault;


import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.base.Strings;
import com.streamsets.datacollector.vault.api.Authenticate;
import com.streamsets.datacollector.vault.api.Logical;
import com.streamsets.datacollector.vault.api.VaultException;
import com.streamsets.datacollector.vault.api.sys.Sys;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public class VaultClient {
  private static final String X509 = "SunX509";
  private static final String TLS = "TLS";

  private final Authenticate authenticate;
  private final Logical logical;
  private final Sys sys;

  public VaultClient(VaultConfiguration conf) {
    NetHttpTransport.Builder transportBuilder = new NetHttpTransport.Builder()
        .setProxy(getProxy(conf.getProxyOptions()));

    if (!conf.getSslOptions().isSslVerify()) {
      try {
        transportBuilder.doNotValidateCertificate();
      } catch (GeneralSecurityException e) {
        throw new VaultRuntimeException("Error disabling SSL certificate verification: " + e.toString(), e);
      }
    } else if (conf.getAddress().startsWith("https")) {
      transportBuilder.setSslSocketFactory(getSSLSocketFactory(conf.getSslOptions()));
    }

    HttpTransport httpTransport = transportBuilder.build();
    try {
      this.authenticate = new Authenticate(conf, httpTransport);
      this.logical = new Logical(conf, httpTransport);
      this.sys = new Sys(conf, httpTransport);
    } catch (VaultException e) {
      throw new VaultRuntimeException(e);
    }
  }

  private SSLSocketFactory getSSLSocketFactory(final SslOptions sslOptions) {
    try {
      final KeyStore trustStore = KeyStore.getInstance("jks");

      SSLContext sslContext = SSLContext.getInstance(TLS);

      TrustManager[] trustManagers = getTrustManagers(
          Strings.isNullOrEmpty(sslOptions.getTrustStoreFile()) ? null : trustStore
      );
      sslContext.init(getKeyManagers(trustStore, sslOptions), trustManagers, null);
      sslContext.getDefaultSSLParameters().setProtocols(sslOptions.getEnabledProtocols().split(","));

      return sslContext.getSocketFactory();
    } catch (IOException | GeneralSecurityException e) {
      throw new VaultRuntimeException("Failed to initialize SSL: " + e.toString(), e);
    }
  }

  private TrustManager[] getTrustManagers(KeyStore keystore) throws GeneralSecurityException {
    TrustManager[] trustManagers = new TrustManager[1];
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(X509);
    trustManagerFactory.init(keystore);
    for (TrustManager trustManager1 : trustManagerFactory.getTrustManagers()) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManagers[0] = trustManager1;
        break;
      }
    }
    return trustManagers;
  }

  private KeyManager[] getKeyManagers(final KeyStore trustStore, SslOptions sslOptions)
      throws GeneralSecurityException, IOException {

    if (sslOptions.getTrustStoreFile() != null && !sslOptions.getTrustStoreFile().isEmpty()) {
      try (InputStream is = new FileInputStream(sslOptions.getTrustStoreFile())) {
        trustStore.load(is, sslOptions.getTrustStorePassword().toCharArray());
      }
    } else {
      // Load empty trust store
      trustStore.load(null, null);
    }

    KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(X509);
    keyMgrFactory.init(trustStore, sslOptions.getTrustStorePassword().toCharArray());
    return keyMgrFactory.getKeyManagers();
  }

  private Proxy getProxy(ProxyOptions proxyOptions) {
    if (proxyOptions.getAddress() != null && !proxyOptions.getAddress().isEmpty()) {
      return new Proxy(
          Proxy.Type.HTTP,
          new InetSocketAddress(proxyOptions.getAddress(), proxyOptions.getPort())
      );
    } else {
      return null;
    }
  }

  public Authenticate authenticate() {
    return authenticate;
  }

  public Logical logical() {
    return logical;
  }

  public Sys sys() {
    return sys;
  }

}
