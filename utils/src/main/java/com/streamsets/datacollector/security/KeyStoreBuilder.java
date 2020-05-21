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
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Java KeyStore builder.
 */
public class KeyStoreBuilder {
  private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
  private static final String END_CERT = "-----END CERTIFICATE-----";
  private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
  private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

  private final Map<String, String> certificatePems;
  private final Map<String, PrivateKeyInfo> privateKeys;

  public KeyStoreBuilder() {
    this.certificatePems = new HashMap<>();
    this.privateKeys = new HashMap<>();
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
    checkPemFormat(pem, BEGIN_CERT, END_CERT);
    certificatePems.put(alias, pem);
    return this;
  }

  private static class PrivateKeyInfo {
    private String privateKey;
    private String password;
    private List<String> certificateChain;

    PrivateKeyInfo(String privateKey, String password, List<String> certificateChain) {
      this.privateKey = privateKey;
      this.password = password;
      this.certificateChain = certificateChain;
    }

    public String getPrivateKey() {
      return privateKey;
    }

    public String getPassword() {
      return password;
    }

    public List<String> getCertificateChain() {
      return certificateChain;
    }
  }

  public KeyStoreBuilder addPrivateKey(String privateKey, String password, List<String> certificateChain) {
    return addPrivateKey(UUID.randomUUID().toString(), privateKey, password, certificateChain);
  }

  public KeyStoreBuilder addPrivateKey(String alias, String privateKey, String password, List<String> certificateChain) {
    Utils.checkNotNull(alias, "alias");
    Utils.checkNotNull(privateKey, "privateKey");
    Utils.checkNotNull(password, "password");
    Utils.checkNotNull(certificateChain, "certificateChain");
    privateKey = privateKey.trim();
    checkPemFormat(privateKey, BEGIN_PRIVATE_KEY, END_PRIVATE_KEY);
    certificateChain = certificateChain.stream().map(String::trim).collect(Collectors.toList());
    certificateChain.forEach(cert -> checkPemFormat(cert, BEGIN_CERT, END_CERT));
    privateKeys.put(alias, new PrivateKeyInfo(privateKey, password, certificateChain));
    return this;
  }

  private void checkPemFormat(String pem, String begin, String end) {
    Utils.checkArgument(
        pem.startsWith(begin) && pem.endsWith(end),
        "Invalid certificate PEM, missing or incorrect BEGIN/END markers"
    );
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
      for (Map.Entry<String, PrivateKeyInfo> entry : privateKeys.entrySet()) {
        PrivateKeyInfo keyInfo = entry.getValue();
        keystore.setKeyEntry(
            entry.getKey(),
            createPrivateKey(keyInfo.getPrivateKey()),
            keyInfo.getPassword().toCharArray(),
            keyInfo.getCertificateChain().stream().map(this::createCertificate).toArray(Certificate[]::new)
        );
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

    try {
      CertificateFactory factory = CertificateFactory.getInstance("X.509");
      return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_002, ex.toString());
    }
  }

  RSAPrivateKey createPrivateKey(String pem) {
    String b64 = pem.replace(BEGIN_PRIVATE_KEY, "").replace(END_PRIVATE_KEY, "");

    byte[] keyBytes;
    try {
      keyBytes = Base64.getMimeDecoder().decode(b64);
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_001, ex.toString());
    }

    try {
      KeyFactory factory = KeyFactory.getInstance("RSA");
      EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
      return (RSAPrivateKey) factory.generatePrivate(keySpec);
    } catch (Exception ex) {
      throw new StageException(Errors.SECURITY_006, ex.toString());
    }
  }
}
