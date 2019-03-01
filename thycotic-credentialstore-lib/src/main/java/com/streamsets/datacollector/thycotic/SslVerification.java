/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.thycotic;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.base.Strings;

public class SslVerification {

  private static final String X509 = "SunX509";
  private static final String TLS = "TLS";

  public SslVerification(ThycoticConfiguration conf) {
    NetHttpTransport.Builder transportBuilder = new NetHttpTransport.Builder();

    if (!conf.getSslOptions().isSslVerify()) {
      try {
        transportBuilder.doNotValidateCertificate();
      } catch (GeneralSecurityException e) {
        throw new ThycoticRuntimeException("Error disabling SSL certificate verification: " + e.toString(), e);
      }
    } else if (conf.getAddress().startsWith("https")) {
      transportBuilder.setSslSocketFactory(getSSLSocketFactory(conf.getSslOptions()));
    }
  }

  private SSLSocketFactory getSSLSocketFactory(final SslOptions sslOptions) {
    try {
      final KeyStore trustStore = KeyStore.getInstance("jks");

      SSLContext sslContext = SSLContext.getInstance(TLS);

      TrustManager[] trustManagers = getTrustManagers(Strings.isNullOrEmpty(sslOptions.getTrustStoreFile()) ? null :
          trustStore);
      sslContext.init(getKeyManagers(trustStore, sslOptions), trustManagers, null);
      sslContext.getDefaultSSLParameters().setProtocols(sslOptions.getEnabledProtocols().split(","));

      return sslContext.getSocketFactory();
    } catch (IOException | GeneralSecurityException e) {
      throw new ThycoticRuntimeException("Failed to initialize SSL: " + e.toString(), e);
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

  private KeyManager[] getKeyManagers(final KeyStore trustStore, SslOptions sslOptions) throws
      GeneralSecurityException,
      IOException {

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
}
