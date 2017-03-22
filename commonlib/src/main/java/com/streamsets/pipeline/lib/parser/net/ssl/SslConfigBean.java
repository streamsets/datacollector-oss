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
 *     http://www.apache.org/licenses/LICENSE_2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.lib.parser.net.ssl;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.VaultEL;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.log4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SslConfigBean {

  private static final Logger LOGGER = Logger.getLogger(SslConfigBean.class);

  private static final String[] SSL_PROTOCOLS = {"TLSv1.2", "TLSv1.1"};
  private static final String[] SSL_CIPHER_SUITES = {
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"
  };

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "Certificate/Key Type",
      description = "The type of SSL certificate/key scheme to use",
      displayPosition = 10,
      group = "#0",
      dependsOn = "tlsEnabled^",
      triggeredByValue = "true"
  )
  @ValueChooserModel(CertificateTypeChooserValues.class)
  public CertificateType certificateType = CertificateType.JKS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Certificate Chain File",
      description = "The X.509 certificate chain file in PEM format. Absolute path, or relative to the Data " +
          "Collector resources directory",
      displayPosition = 20,
      group = "#0",
      dependsOn = "certificateType",
      triggeredByValue = "PKCS8"
  )
  public String certificateChainFile;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Private Key File",
      description = "The private key PEM file in PKCS8 format. Absolute path, or relative to the Data Collector " +
          "resources directory.",
      displayPosition = 30,
      group = "#0",
      dependsOn = "certificateType",
      triggeredByValue = "PKCS8"
  )
  public String privateKeyFile;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "The private key encryption password for the PKCS8 key. Leave blank if no password is used (not " +
          "recommended).",
      label = "Private key encryption password",
      displayPosition = 40,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "certificateType",
      triggeredByValue = "PKCS8"
  )
  public String keyPassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      description = "The path to the keystore file.  Absolute path, or relative to the Data Collector resources " +
          "directory.",
      label = "Keystore file",
      displayPosition = 50,
      group = "#0",
      dependsOn = "certificateType",
      triggeredByValue = {"JKS", "PKCS12"}
  )
  public String keystoreFilePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "The password to the keystore file. May not be used with 'Keystore password file' option.",
      label = "Keystore password",
      displayPosition = 70,
      elDefs = VaultEL.class,
      group = "#0",
      dependsOn = "certificateType",
      triggeredByValue = {"JKS", "PKCS12"}
  )
  public String keystoreFilePassword;

  private SslContext nettySslContext;
  private SSLEngine sslEngine;

  public boolean init(
      Stage.Context context,
      String groupName,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    boolean valid = true;

    switch (certificateType) {
      case JKS:
        valid = buildSslContextFromKeystore(
            "JKS",
            "Specified ",
            keystoreFilePath,
            keystoreFilePassword,
            context,
            context.getResourcesDirectory(),
            "keystoreFilePath",
            groupName,
            configPrefix,
            issues
        );
        break;
      case PKCS8:
        final Path certFilePath = getFilePath(context.getResourcesDirectory(), certificateChainFile);
        if (!Files.exists(certFilePath)) {
          issues.add(context.createConfigIssue(
              groupName,
              configPrefix + "certificateChainFile",
              SslConfigErrors.SSL_10,
              "certificateChainFile",
              certFilePath
          ));
          valid = false;
        }
        try {
          nettySslContext = SslContextBuilder.forServer(
              Files.newInputStream(getFilePath(context.getResourcesDirectory(), certificateChainFile)),
              Files.newInputStream(getFilePath(context.getResourcesDirectory(), privateKeyFile)),
              keyPassword
          ).build();
        } catch (IOException e) {
          LOGGER.error("IOException attempting to load password file", e);
          issues.add(context.createConfigIssue(
              groupName,
              "certificateChainFile",
              SslConfigErrors.SSL_30,
              certificateChainFile,
              e.getMessage(),
              e
          ));
          valid = false;
        }

        break;
      case PKCS12:
        valid = buildSslContextFromKeystore(
            "PKCS12",
            "Specified ",
            keystoreFilePath,
            keystoreFilePassword,
            context,
            context.getResourcesDirectory(),
            "keystoreFilePath",
            groupName,
            configPrefix,
            issues
        );
        break;
    }

    return valid;
  }

  private boolean buildSslContextFromKeystore(
      String keystoreType,
      String keystoreDesc,
      String keystoreFile,
      String password,
      Stage.Context context,
      String defaultFileDirectory,
      String keystoreFilePathProperty,
      String groupName,
      String configPrefix,
      List<Stage.ConfigIssue> issues
  ) {
    final Path keystorePath = getFilePath(defaultFileDirectory, keystoreFile);
    final String ksProp = keystoreFilePathProperty == null ? null : configPrefix + keystoreFilePathProperty;
    if (!Files.exists(keystorePath)) {
      issues.add(context.createConfigIssue(
          groupName,
          ksProp,
          SslConfigErrors.SSL_01,
          keystoreDesc,
          keystoreFile
      ));
      return false;
    }

    char[] passwordChars = null;

    if (!Strings.isNullOrEmpty(password)) {
      passwordChars = password.toCharArray();
    }

    try {
      KeyStore ks = KeyStore.getInstance(keystoreType);
      ks.load(Files.newInputStream(keystorePath), passwordChars);

      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(ks, passwordChars);

      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(kmf.getKeyManagers(), null, null);

      sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(false);
      sslEngine.setNeedClientAuth(false);
      sslEngine.setEnabledProtocols(getSupportedValuesFromWhitelist(sslEngine.getSupportedProtocols(), SSL_PROTOCOLS));
      sslEngine.setEnabledCipherSuites(getSupportedValuesFromWhitelist(
          sslEngine.getSupportedCipherSuites(),
          SSL_CIPHER_SUITES
      ));
      sslEngine.setEnableSessionCreation(true);

      return true;
    } catch (Exception e) {
      LOGGER.error("Exception attempting to initialize keystore", e);
      issues.add(context.createConfigIssue(
          groupName, ksProp,
          SslConfigErrors.SSL_20,
          e.getMessage(),
          e
      ));
      return false;
    }
  }

  public void bindToChannel(Channel channel) {
    if (nettySslContext != null) {
      channel.pipeline().addFirst("SSL", nettySslContext.newHandler(channel.alloc()));
    } else if (sslEngine != null) {
      channel.pipeline().addFirst("SSL", new SslHandler(sslEngine));
    } else {
      throw new IllegalStateException("nettySslContext and sslEngine were both null");
    }
  }

  private static Path getFilePath(String resourcesDir, String path) {
    final Path p = Paths.get(path);
    if (p.isAbsolute()) {
      return p;
    } else {
      return Paths.get(resourcesDir, path);
    }
  }

  private String[] getSupportedValuesFromWhitelist(String[] supportedValues, String[] whitelistValues) {
    final Set<String> supportedSet = new HashSet<>(Arrays.asList(supportedValues));
    final Set<String> whitelistSet = new HashSet<>(Arrays.asList(whitelistValues));

    return Sets.intersection(supportedSet, whitelistSet).toArray(new String[0]);

  }

}
