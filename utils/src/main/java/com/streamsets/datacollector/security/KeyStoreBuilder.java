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
package com.streamsets.datacollector.security;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Java KeyStore builder.
 */
public class KeyStoreBuilder {
  private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
  private static final String END_CERT = "-----END CERTIFICATE-----";

  private final Map<String, String> certificatePems;

  public KeyStoreBuilder() {
    this.certificatePems = new HashMap<>();
  }

  /**
   * Adds a PEM certificate to the builder with a random alias.
   */
  public KeyStoreBuilder addCertificatePem(String pem) {
    return addCertificatePem(UUID.randomUUID().toString(), pem);
  }

  /**
   * Adds a PEM certificate to the builder with a given alias.
   */
  public KeyStoreBuilder addCertificatePem(String alias, String pem) {
    Utils.checkNotNull(alias, "alias");
    Utils.checkNotNull(pem, "pem");
    pem = pem.trim();
    Utils.checkArgument(
        pem.startsWith(BEGIN_CERT) && pem.endsWith(END_CERT),
        "Invalid certificate PEM, missing or incorrect BEGIN/END markers"
    );
    certificatePems.put(alias, pem);
    return this;
  }

  /**
   * Creates the Java KeyStore containing all the certificates given to the builder.
   */
  public KeyStore build() {
    KeyStore keystore;
    try {
      keystore = KeyStore.getInstance("JKS");
      keystore.load(null);
      for (Map.Entry<String, String> entry : certificatePems.entrySet()) {
        keystore.setCertificateEntry(entry.getKey(), createCertificate(entry.getValue()));
      }
      return keystore;
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_003, ex.toString());
    }
  }

  /**
   * Converts a PEM certificate into a X509Certificate instance.
   */
  X509Certificate createCertificate(String pem) {
    String b64 = pem.replace(BEGIN_CERT, "").replace(END_CERT, "");

    byte[] certBytes;
    try {
      certBytes = Base64.getMimeDecoder().decode(b64);
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_001, ex.toString());
    }
    X509Certificate cert;
    try {
      CertificateFactory factory = CertificateFactory.getInstance("X.509");
      return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_002, ex.toString());
    }
  }

}
