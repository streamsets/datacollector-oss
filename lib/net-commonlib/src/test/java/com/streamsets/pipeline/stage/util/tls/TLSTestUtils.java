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
package com.streamsets.pipeline.stage.util.tls;

import org.bouncycastle.x509.X509V1CertificateGenerator;

import javax.security.auth.x500.X500Principal;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;

public class TLSTestUtils {

  public static X509Certificate generateCertificate(String dn, KeyPair keyPair, int days) throws Exception {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000L);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm("SHA1withRSA");

    return certGen.generate(keyPair.getPrivate());
  }

  public static KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename, String password)  throws Exception {
    try (FileOutputStream out = new FileOutputStream(filename)) {
      ks.store(out, password.toCharArray());
    }
  }

  public static void createKeyStore(String filename, String password, String alias, Key privateKey, Certificate cert)
      throws Exception {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(), new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  public static void createTrustStore(String filename, String password, String alias, Certificate cert)
      throws Exception {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    saveKeyStore(ks, filename, password);
  }

  public static String getHostname() throws Exception {
    return InetAddress.getLocalHost().getHostName();
  }
}
