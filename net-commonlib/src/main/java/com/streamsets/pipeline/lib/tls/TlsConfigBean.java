/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.lib.tls;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.VaultEL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TlsConfigBean {

  public static final String DEFAULT_KEY_MANAGER_ALGORITHM = "SunX509";

  private static final String[] MODERN_PROTOCOLS = {"TLSv1.2"};
  private static final String[] MODERN_CIPHER_SUITES = {
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"
  };

  private static final Logger LOGGER = Logger.getLogger(TlsConfigBean.class);

  public TlsConfigBean() {
    this(TlsConnectionType.NEITHER);
  }

  public TlsConfigBean(TlsConnectionType connectionType) {
    switch (connectionType) {
      case NEITHER:
        this.hasKeyStore = false;
        this.hasTrustStore = false;
        break;
      case CLIENT:
        this.hasKeyStore = false;
        this.hasTrustStore = true;
        break;
      case SERVER:
        this.hasKeyStore = true;
        this.hasTrustStore = false;
        break;
      case BOTH:
        this.hasKeyStore = true;
        this.hasTrustStore = true;
        break;
    }
  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Key Store",
      description = "Use a key store to manage server-side private keys.",
      displayPosition = 10,
      group = "#0",
      dependsOn = "tlsEnabled^",
      triggeredByValue = "true"
  )
  public boolean hasKeyStore = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "Key Store Type",
      description = "The type of certificate/key scheme to use for the key store.",
      displayPosition = 20,
      group = "#0",
      dependsOn = "hasKeyStore",
      triggeredByValue = "true"
  )
  @ValueChooserModel(KeyStoreTypeChooserValues.class)
  public KeyStoreType keyStoreType = KeyStoreType.JKS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      description = "The path to the key store file.  Absolute path, or relative to the Data Collector resources "
          + "directory.",
      label = "Key Store File",
      displayPosition = 50,
      group = "#0",
      dependsOn = "hasKeyStore",
      triggeredByValue = "true"
  )
  public String keyStoreFilePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "The password to the key store file, if applicable.  Using a password is highly recommended for"
          + "security reasons.",
      label = "Key Store Password",
      displayPosition = 70,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "hasKeyStore",
      triggeredByValue = "true"
  )
  public String keyStorePassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Key Store Key Algorithm",
      description = "The key manager algorithm to use with the key store.",
      defaultValue = DEFAULT_KEY_MANAGER_ALGORITHM,
      displayPosition = 80,
      group = "#0",
      dependsOn = "hasKeyStore",
      triggeredByValue = "true"
  )
  public String keyStoreAlgorithm = DEFAULT_KEY_MANAGER_ALGORITHM;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Trust Store",
      description = "Use a trust store to manage client-side certificates.",
      displayPosition = 100,
      group = "#0",
      dependsOn = "tlsEnabled^",
      triggeredByValue = "true"
  )
  public boolean hasTrustStore = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "Trust Store Type",
      description = "The type of certificate/key scheme to use for the trust store.",
      displayPosition = 120,
      group = "#0",
      dependsOn = "hasTrustStore",
      triggeredByValue = "true"
  )
  @ValueChooserModel(KeyStoreTypeChooserValues.class)
  public KeyStoreType trustStoreType = KeyStoreType.JKS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      description = "The path to the trust store file.  Absolute path, or relative to the Data Collector resources "
          + "directory.",
      label = "Trust Store File",
      displayPosition = 150,
      group = "#0",
      dependsOn = "hasTrustStore",
      triggeredByValue = "true"
  )
  public String trustStoreFilePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "The password to the trust store file, if applicable.  Using a password is highly recommended for"
          + "security reasons.",
      label = "Trust Store Password",
      displayPosition = 170,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "hasTrustStore",
      triggeredByValue = "true"
  )
  public String trustStorePassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Trust Store Trust Algorithm",
      description = "The key manager algorithm to use with the trust store.",
      defaultValue = DEFAULT_KEY_MANAGER_ALGORITHM,
      displayPosition = 180,
      group = "#0",
      dependsOn = "hasTrustStore",
      triggeredByValue = "true"
  )
  public String trustStoreAlgorithm = DEFAULT_KEY_MANAGER_ALGORITHM;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Default (Modern) Protocols",
      description = "Use only modern TLS protocols.  This is highly recommended for security reasons, but can be"
          + "overridden if special circumstances require it.",
      defaultValue = "true",
      displayPosition = 300,
      group = "#0",
      dependsOn = "tlsEnabled^",
      triggeredByValue = "true"
  )
  public boolean useDefaultProtocols = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Transport Protocols",
      description = "The transport protocols to enable for connections (ex: TLSv1.2, TLSv1.1, etc.).",
      displayPosition = 310,
      group = "#0",
      dependsOn = "useDefaultProtocols",
      triggeredByValue = "false"
  )
  public List<String> protocols = new LinkedList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Default (Modern) Cipher Suites",
      description = "Use only modern cipher suites.  This is highly recommended for security reasons, but can be" +
          "overridden if special circumstances require it.",
      defaultValue = "true",
      displayPosition = 350,
      group = "#0",
      dependsOn = "tlsEnabled^",
      triggeredByValue = "true"
  )
  public boolean useDefaultCiperSuites = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Cipher Suites",
      description = "The cipher suites for connections (ex: TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, etc.).",
      displayPosition = 360,
      group = "#0",
      dependsOn = "useDefaultProtocols",
      triggeredByValue = "false"
  )
  public List<String> cipherSuites = new LinkedList<>();

  public boolean pathRelativeToResourcesDir = true;

  private SSLEngine sslEngine;
  private SSLContext sslContext;
  private KeyStore keyStore;
  private KeyStore trustStore;

  private Set<String> getSupportedValuesFromSpecified(
      Collection<String> supportedValues, Collection<String> specifiedValues, String type
  ) {
    Set<String> returnSet = new HashSet<>();
    final Set<String> supportedSet = new HashSet<>(supportedValues);

    for (String specified : specifiedValues) {
      if (supportedSet.contains(specified)) {
        returnSet.add(specified);
      } else {
        if (LOGGER.isEnabledFor(Level.WARN)) {
          LOGGER.warn(String.format(
              "%s %s was specified, but is not supported within the JVM; disabling",
              type,
              specified
          ));
        }
      }
    }
    return returnSet;
  }

  private static Path getFilePath(String resourcesDir, String path, boolean pathRelativeToResourcesDir) {
    if (Strings.isNullOrEmpty(path)) {
      return null;
    }
    final Path p = Paths.get(path);
    if (p.isAbsolute() || !pathRelativeToResourcesDir) {
      return p;
    } else {
      return Paths.get(resourcesDir, path);
    }
  }

  public boolean isEitherStoreEnabled() {
    return hasKeyStore || hasTrustStore;
  }

  public boolean init(
      Stage.Context context, String groupName, String configPrefix, List<Stage.ConfigIssue> issues
  ) {
    KeyManagerFactory keyStoreFactory = null;
    TrustManagerFactory trustStoreFactory = null;

    if (!isEitherStoreEnabled()) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "hasKeyStore",
          TlsConfigErrors.TLS_05
      ));
      return false;
    }

    if (hasKeyStore) {
      keyStoreFactory = initializeKeyStore(context, groupName, configPrefix, issues);
      if (keyStoreFactory == null) {
        return false;
      }
    }

    if (hasTrustStore) {
      trustStoreFactory = initializeTrustStore(context, groupName, configPrefix, issues);
      if (trustStoreFactory == null) {
        return false;
      }
    }

    try {
      sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyStoreFactory != null ? keyStoreFactory.getKeyManagers() : null,
          trustStoreFactory != null ? trustStoreFactory.getTrustManagers() : null,
          null
      );
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      issues.add(context.createConfigIssue(
          groupName,
          "trustStoreFilePath",
          TlsConfigErrors.TLS_51,
          e.getMessage(),
          e
      ));
      return false;
    }

    sslEngine = sslContext.createSSLEngine();
    sslEngine.setUseClientMode(false);
    sslEngine.setNeedClientAuth(false);

    Collection<String> filteredProtocols;
    if (useDefaultProtocols) {
      filteredProtocols = getSupportedValuesFromSpecified(Arrays.asList(sslEngine.getSupportedProtocols()),
          Arrays.asList(MODERN_PROTOCOLS),
          "Protocol"
      );
    } else {
      filteredProtocols = getSupportedValuesFromSpecified(Arrays.asList(sslEngine.getSupportedProtocols()),
          protocols,
          "Protocol"
      );
    }
    sslEngine.setEnabledProtocols(filteredProtocols.toArray(new String[0]));

    Collection<String> filteredCipherSuites;
    if (useDefaultCiperSuites) {
      filteredCipherSuites = getSupportedValuesFromSpecified(Arrays.asList(sslEngine.getSupportedCipherSuites()),
          Arrays.asList(MODERN_CIPHER_SUITES),
          "Cipher suite"
      );
    } else {
      filteredCipherSuites = getSupportedValuesFromSpecified(Arrays.asList(sslEngine.getSupportedCipherSuites()),
          cipherSuites,
          "Cipher suite"
      );
    }
    sslEngine.setEnabledCipherSuites(filteredCipherSuites.toArray(new String[0]));

    sslEngine.setEnableSessionCreation(true);
    sslEngine.setUseClientMode(isClientMode());

    return true;
  }

  private KeyManagerFactory initializeKeyStore(
      Stage.Context context,
      String groupName,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    final Path keyStorePath = getFilePath(
        context.getResourcesDirectory(),
        keyStoreFilePath,
        pathRelativeToResourcesDir
    );

    if (keyStorePath == null) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "keyStoreFilePath",
          TlsConfigErrors.TLS_02,
          "Key"
      ));
      return null;
    }

    keyStore = initializeKeyStoreFromConfig(context,
        groupName,
        configPrefix,
        issues,
        keyStorePath,
        keyStorePassword,
        keyStoreType,
        "Key"
    );
    if (keyStore == null) {
      return null;
    }

    KeyManagerFactory kmf;
    try {
      kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
    } catch (NoSuchAlgorithmException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "keyStoreAlgorithm",
          TlsConfigErrors.TLS_22,
          keyStoreAlgorithm,
          e.getMessage(),
          e
      ));
      return null;
    }

    try {
      kmf.init(keyStore, getPasswordChars(keyStorePassword));
    } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "keyStoreFilePath",
          TlsConfigErrors.TLS_23,
          e.getMessage(),
          e
      ));
      return null;
    }
    return kmf;
  }

  private TrustManagerFactory initializeTrustStore(
      Stage.Context context,
      String groupName,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    final Path trustStorePath = getFilePath(
        context.getResourcesDirectory(),
        trustStoreFilePath,
        pathRelativeToResourcesDir
    );

    if (trustStorePath == null) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "trustStoreFilePath",
          TlsConfigErrors.TLS_02,
          "Trust"
      ));
      return null;
    }

    trustStore = initializeKeyStoreFromConfig(
        context,
        groupName,
        configPrefix,
        issues,
        trustStorePath,
        trustStorePassword,
        trustStoreType,
        "Trust"
    );
    if (trustStore == null) {
      return null;
    }

    TrustManagerFactory tmf;
    try {
      tmf = TrustManagerFactory.getInstance(trustStoreAlgorithm);
    } catch (NoSuchAlgorithmException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "trustStoreAlgorithm",
          TlsConfigErrors.TLS_50,
          trustStoreAlgorithm,
          e.getMessage(),
          e
      ));
      return null;
    }

    try {
      tmf.init(trustStore);
    } catch (KeyStoreException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + "trustStoreFilePath",
          TlsConfigErrors.TLS_51,
          e.getMessage(),
          e
      ));
      return null;
    }
    return tmf;
  }

  public boolean isClientMode() {
    return hasTrustStore && !hasKeyStore;
  }

  public KeyStore getKeyStore() {
    return keyStore;
  }

  public KeyStore getTrustStore() {
    return trustStore;
  }

  public SSLContext getSslContext() {
    return sslContext;
  }

  public SSLEngine getSslEngine() {
    return sslEngine;
  }

  private static KeyStore initializeKeyStoreFromConfig(
      Stage.Context context,
      String groupName,
      String configPrefix,
      List<Stage.ConfigIssue> issues,
      Path keyStorePath,
      String password,
      KeyStoreType type,
      String storeCategory
  ) {
    if (!keyStorePath.toFile().exists()) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + storeCategory.toLowerCase() + "StoreFilePath",
          TlsConfigErrors.TLS_01,
          storeCategory,
          keyStorePath
      ));
      return null;
    }

    KeyStore ks;
    try {
      ks = KeyStore.getInstance(type.getJavaValue());
    } catch (KeyStoreException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + storeCategory.toLowerCase() + "StoreType",
          TlsConfigErrors.TLS_20,
          storeCategory,
          e.getMessage(),
          e
      ));
      return null;
    }

    try (final InputStream keyStoreIs = Files.newInputStream(keyStorePath)) {
      ks.load(keyStoreIs, getPasswordChars(password));
    } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
      issues.add(context.createConfigIssue(
          groupName,
          configPrefix + storeCategory.toLowerCase() + "StoreFilePath",
          TlsConfigErrors.TLS_21,
          storeCategory,
          keyStorePath,
          e.getMessage(),
          e
      ));
      return null;
    }

    return ks;
  }

  private static char[] getPasswordChars(String password) {
    if (Strings.isNullOrEmpty(password)) {
      return new char[0];
    } else {
      return password.toCharArray();
    }
  }
}
