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
package com.streamsets.pipeline.lib.remote;

import com.streamsets.datacollector.security.KeyStoreBuilder;
import com.streamsets.pipeline.api.ConfigIssueContext;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.stage.connection.remote.FTPSTrustStore;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import org.apache.commons.net.util.KeyManagerUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Handles all of the logic about connecting to an FTP server, including config verification.  Subclasses can get
 * this functionality for free by simply calling {@link #initAndConnect(List, ConfigIssueContext, URI, Label, Label)}
 * and making sure {@link RemoteConfigBean} is setup properly.
 */
public abstract class FTPRemoteConnector extends RemoteConnector {

  private static final Logger LOG = LoggerFactory.getLogger(FTPRemoteConnector.class);
  private static final String FTP_SCHEME = "ftp";
  private static final String FTPS_SCHEME = "ftps";
  public static final int DEFAULT_PORT = 21;
  private static final Pattern URL_PATTERN_FTP = Pattern.compile("(ftp://).*:?.*");
  private static final Pattern URL_PATTERN_FTPS = Pattern.compile("(ftps://).*:?.*");

  protected FileSystemOptions options;
  protected URI remoteURI;
  protected FileObject remoteDir;

  protected FTPRemoteConnector(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  @Override
  protected void initAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      URI remoteURI,
      Label remoteGroup,
      Label credGroup
  ) {
    LOG.info("Starting connection to remote server");
    options = new FileSystemOptions();
    this.remoteURI = remoteURI;
    switch (remoteConfig.connection.credentials.auth) {
      case PRIVATE_KEY:
        issues.add(context.createConfigIssue(credGroup.getLabel(), CONF_PREFIX + "privateKey", Errors.REMOTE_06));
        break;
      case PASSWORD:
        String username = resolveUsername(remoteURI, issues, context, credGroup);
        String password = resolvePassword(remoteURI, issues, context, credGroup);
        if (remoteURI.getUserInfo() != null) {
          remoteURI = UriBuilder.fromUri(remoteURI).userInfo(null).build();
        }
        StaticUserAuthenticator authenticator = new StaticUserAuthenticator(remoteURI.getHost(), username, password);
        try {
          DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, authenticator);
        } catch (FileSystemException e) {
          // This method doesn't actually ever throw a FileSystemException, it simply declares that it does...
          // This shouldn't happen, but just in case, we'll log it
          LOG.error("Unexpected Exception", e);
        }
        break;
      default:
        break;
    }

    FtpFileSystemConfigBuilder configBuilder = FtpFileSystemConfigBuilder.getInstance();
    if (remoteURI.getScheme() != null) {
      if (remoteURI.getScheme().equals(FTPS_SCHEME)) {
        configBuilder = FtpsFileSystemConfigBuilder.getInstance();
        FtpsFileSystemConfigBuilder.getInstance().setFtpsMode(options, remoteConfig.connection.ftpsMode.getMode());
        FtpsFileSystemConfigBuilder.getInstance().setDataChannelProtectionLevel(options,
            remoteConfig.connection.ftpsDataChannelProtectionLevel.getLevel()
        );
        if (remoteConfig.connection.credentials.useFTPSClientCert) {
          String keyStorePassword = resolveCredential(remoteConfig.ftpsClientCertKeystorePassword,
              CONF_PREFIX + "ftpsClientCertKeystorePassword",
              issues,
              context,
              credGroup
          );
          KeyStore keyStore = null;
          if (remoteConfig.useRemoteKeyStore) {
            if (remoteConfig.ftpsCertificateChain.isEmpty()) {
              issues.add(context.createConfigIssue(credGroup.getLabel(),
                  CONF_PREFIX + "ftpsCertificateChain",
                  Errors.REMOTE_16
              ));
            } else {
              keyStore = new KeyStoreBuilder().addCertificatePem(remoteConfig.ftpsCertificateChain.get(0).get()).addPrivateKey(remoteConfig.ftpsPrivateKey.get(),
                  "",
                  remoteConfig.ftpsCertificateChain.stream().map(CredentialValueBean::get).collect(Collectors.toList())
              ).build();
            }
          } else {
            keyStore = loadKeyStore(remoteConfig.ftpsClientCertKeystoreFile,
                CONF_PREFIX + "ftpsClientCertKeystoreFile",
                keyStorePassword,
                remoteConfig.ftpsClientCertKeystoreType,
                true,
                issues,
                context,
                credGroup
            );
          }
          if (keyStore != null) {
            setFtpsUserKeyManagerOrTrustManager(keyStore,
                CONF_PREFIX + "ftpsClientCertKeystoreFile",
                keyStorePassword,
                true,
                issues,
                context,
                credGroup
            );
          }
        }
        String trustStorePassword = resolveCredential(remoteConfig.ftpsTruststorePassword,
            CONF_PREFIX + "ftpsTruststorePassword",
            issues,
            context,
            credGroup
        );

        switch (remoteConfig.connection.credentials.ftpsTrustStoreProvider) {
          case FILE:
          case REMOTE_TRUSTSTORE:
            KeyStore trustStore;
            if (remoteConfig.connection.credentials.ftpsTrustStoreProvider == FTPSTrustStore.REMOTE_TRUSTSTORE) {
              KeyStoreBuilder builder = new KeyStoreBuilder();
              remoteConfig.ftpsTrustedCertificates.forEach(cert -> builder.addCertificatePem(cert.get()));
              trustStore = builder.build();
            } else {
              trustStore = loadKeyStore(remoteConfig.ftpsTruststoreFile,
                  CONF_PREFIX + "ftpsTruststoreFile",
                  trustStorePassword,
                  remoteConfig.ftpsClientCertKeystoreType,
                  false,
                  issues,
                  context,
                  credGroup
              );
            }
            setFtpsUserKeyManagerOrTrustManager(trustStore,
                CONF_PREFIX + "ftpsTruststoreFile",
                trustStorePassword,
                false,
                issues,
                context,
                credGroup
            );
            break;
          case JVM_DEFAULT:
            try {
              FtpsFileSystemConfigBuilder.getInstance().setTrustManager(options,
                  TrustManagerUtils.getDefaultTrustManager(null)
              );
            } catch (GeneralSecurityException e) {
              issues.add(context.createConfigIssue(credGroup.getLabel(),
                  CONF_PREFIX + "ftpsTruststoreFile",
                  Errors.REMOTE_14,
                  "trust",
                  "JVM",
                  e.getMessage(),
                  e
              ));
            }
            break;
          case ALLOW_ALL:
            // fall through
          default:
            FtpsFileSystemConfigBuilder.getInstance().setTrustManager(options,
                TrustManagerUtils.getAcceptAllTrustManager()
            );
            break;
        }
      }
    } else {
      issues.add(
          context.createConfigIssue(
              credGroup.getLabel(),
              CONF_PREFIX + "remoteAddress",
              Errors.REMOTE_18,
              remoteConfig.connection.remoteAddress
          )
      );
    }

    configBuilder.setPassiveMode(options, true);
    configBuilder.setUserDirIsRoot(options, remoteConfig.userDirIsRoot);

    // VFS uses null to indicate no timeout (whereas we use 0)
    if (remoteConfig.socketTimeout > 0) {
      configBuilder.setSoTimeout(options, remoteConfig.socketTimeout * 1000);
    }
    if (remoteConfig.connectionTimeout > 0) {
      configBuilder.setConnectTimeout(options, remoteConfig.connectionTimeout * 1000);
    }
    if (remoteConfig.dataTimeout > 0) {
      configBuilder.setDataTimeout(options, remoteConfig.dataTimeout * 1000);
    }

    if (remoteConfig.connection.protocol == Protocol.FTP
        && !URL_PATTERN_FTP.matcher(remoteConfig.connection.remoteAddress).matches()
        || remoteConfig.connection.protocol == Protocol.FTPS
        && !URL_PATTERN_FTPS.matcher(remoteConfig.connection.remoteAddress).matches()) {
      issues.add(
          context.createConfigIssue(
              credGroup.getLabel(),
              CONF_PREFIX + "remoteAddress",
              Errors.REMOTE_17,
              remoteConfig.connection.remoteAddress,
              remoteConfig.connection.protocol
          )
      );
    }

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI);
      try {
        remoteDir = VFS.getManager().resolveFile(remoteURI.toString(), options);
        remoteDir.refresh();
        if (remoteConfig.createPathIfNotExists && !remoteDir.exists()) {
          remoteDir.createFolder();
        }
        remoteDir.getChildren();
      } catch (FileSystemException e) {
        LOG.error(Errors.REMOTE_11.getMessage(), remoteURI.toString(), e.getMessage(), e);
        issues.add(context.createConfigIssue(
            remoteGroup.getLabel(),
            CONF_PREFIX + "remoteAddress",
            Errors.REMOTE_11,
            remoteURI.toString(),
            e.getMessage(),
            e
        ));
      }
    }
  }

  @Override
  public void verifyAndReconnect() {
    boolean done = false;
    int retryCounter = 0;
    boolean reconnect = false;
    while (!done && retryCounter < MAX_RETRIES) {
      try {
        LOG.info("Trying to reconnect to remote server");
        if (reconnect) {
          remoteDir = VFS.getManager().resolveFile(remoteURI.toString(), options);
          reconnect = false;
        }
        remoteDir.refresh();
        // A call to getChildren() and then refresh() is needed in order to properly refresh if files were updated
        // A possible bug in VFS?
        remoteDir.getChildren();
        remoteDir.refresh();
        done = true;

        LOG.info("Reconnected successfully to remote server");
      } catch (FileSystemException fse) {
        // Refresh can fail due to session is down, a timeout, etc; so try getting a new connection
        if (retryCounter < MAX_RETRIES - 1) {
          LOG.info("Got FileSystemException when trying to refresh remote directory. '{}'", fse.getMessage(), fse);
          LOG.warn("Retrying connection to remote directory");
          reconnect = true;
        } else {
          throw new StageException(Errors.REMOTE_09, fse.getMessage(), fse);
        }
      }
      retryCounter++;
    }
  }

  @Override
  public void close() throws IOException {
    if (remoteDir != null) {
      remoteDir.close();
      FileSystem fs = remoteDir.getFileSystem();
      remoteDir.getFileSystem().getFileSystemManager().closeFileSystem(fs);
    }
  }

  protected String relativizeToRoot(String path) {
    return "/" + Paths.get(remoteDir.getName().getPath()).relativize(Paths.get(path)).toString();
  }

  protected FileObject resolveChild(String path) throws IOException {
    // VFS doesn't properly resolve the child if we don't remove the leading slash, even though that's inconsistent
    // with its other methods
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return remoteDir.resolveFile(path, NameScope.DESCENDENT);
  }

  private void setFtpsUserKeyManagerOrTrustManager(
      KeyStore keystore,
      String fileConfigName,
      String keyStorePassword,
      boolean isKeyStore, // or truststore
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    try {
      if (isKeyStore) {
        FtpsFileSystemConfigBuilder.getInstance().setKeyManager(
            options,
            KeyManagerUtils.createClientKeyManager(keystore, null, keyStorePassword)
        );
      } else {
        FtpsFileSystemConfigBuilder.getInstance().setTrustManager(
            options,
            TrustManagerUtils.getDefaultTrustManager(keystore)
        );
      }
    } catch (GeneralSecurityException e) {
      issues.add(context.createConfigIssue(group.getLabel(),
          fileConfigName,
          Errors.REMOTE_15,
          isKeyStore ? "key" : "trust",
          e.getMessage(),
          e
      ));
    }
  }

  private KeyStore loadKeyStore(
      String file,
      String fileConfigName,
      String password,
      KeyStoreType keyStoreType,
      boolean isKeystore,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    KeyStore keyStore;
    if (file != null && !file.isEmpty()) {
      File keystoreFile = new File(file);
      try {
        keyStore = KeyStore.getInstance(keyStoreType.getJavaValue());
        char[] passwordArr = password != null ? password.toCharArray() : null;
        try (FileInputStream fin = new FileInputStream(file)) {
          keyStore.load(fin, passwordArr);
        }
      } catch (IOException | GeneralSecurityException e) {
        keyStore = null;
        issues.add(context.createConfigIssue(
            group.getLabel(),
            fileConfigName,
            Errors.REMOTE_14,
            isKeystore ? "key" : "trust",
            keystoreFile.getAbsolutePath(),
            e.getMessage(),
            e
        ));
      }
    } else {
      keyStore = null;
      if (isKeystore) {
        issues.add(context.createConfigIssue(group.getLabel(), fileConfigName, Errors.REMOTE_12));
      } else {
        issues.add(context.createConfigIssue(group.getLabel(), fileConfigName, Errors.REMOTE_13));
      }
    }
    return keyStore;
  }
}
