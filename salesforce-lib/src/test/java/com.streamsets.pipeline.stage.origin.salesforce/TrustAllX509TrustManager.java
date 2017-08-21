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
package com.streamsets.pipeline.stage.origin.salesforce;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

class TrustAllX509TrustManager extends X509ExtendedTrustManager {
  public X509Certificate[] getAcceptedIssuers() {
    return new X509Certificate[0];
  }

  @Override
  public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
      String authType) {
  }

  @Override
  public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
      String authType) {
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws
      CertificateException {
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws
      CertificateException {
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws
      CertificateException {
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws
      CertificateException {
  }
}
