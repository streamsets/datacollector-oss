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

import com.streamsets.pipeline.api.ConfigIssueContext;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
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

  protected FileSystemOptions options;
  protected URI remoteURI;
  protected FileObject remoteDir;

  protected FTPRemoteConnector(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  public static boolean handlesScheme(String scheme) {
    return FTP_SCHEME.equals(scheme) || FTPS_SCHEME.equals(scheme);
  }

  @Override
  protected void initAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      URI remoteURI,
      Label remoteGroup,
      Label credGroup
  ) {
    options = new FileSystemOptions();
    this.remoteURI = remoteURI;
    if (remoteConfig.strictHostChecking) {
      issues.add(context.createConfigIssue(credGroup.getLabel(), CONF_PREFIX + "strictHostChecking", Errors.REMOTE_07));
    }
    switch (remoteConfig.auth) {
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
      case NONE:
        break;
      default:
        break;
    }

    FtpFileSystemConfigBuilder configBuilder = FtpFileSystemConfigBuilder.getInstance();
    if (remoteURI.getScheme().equals(FTPS_SCHEME)) {
      configBuilder = FtpsFileSystemConfigBuilder.getInstance();
      FtpsFileSystemConfigBuilder.getInstance().setFtpsMode(options, remoteConfig.ftpsMode.getMode());
      FtpsFileSystemConfigBuilder.getInstance().setDataChannelProtectionLevel(
          options,
          remoteConfig.ftpsDataChannelProtectionLevel.getLevel()
      );
      if (remoteConfig.useFTPSClientCert) {
        setFtpsUserKeyManagerOrTrustManager(
            remoteConfig.ftpsClientCertKeystoreFile,
            CONF_PREFIX + "ftpsClientCertKeystoreFile",
            remoteConfig.ftpsClientCertKeystorePassword,
            CONF_PREFIX + "ftpsClientCertKeystorePassword",
            remoteConfig.ftpsClientCertKeystoreType,
            true,
            issues,
            context,
            credGroup
        );
      }
      switch (remoteConfig.ftpsTrustStoreProvider) {
        case FILE:
          setFtpsUserKeyManagerOrTrustManager(
              remoteConfig.ftpsTruststoreFile,
              CONF_PREFIX + "ftpsTruststoreFile",
              remoteConfig.ftpsTruststorePassword,
              CONF_PREFIX + "ftpsTruststorePassword",
              remoteConfig.ftpsTruststoreType,
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
            issues.add(context.createConfigIssue(
                credGroup.getLabel(),
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
          FtpsFileSystemConfigBuilder.getInstance().setTrustManager(
              options,
              TrustManagerUtils.getAcceptAllTrustManager()
          );
          break;
      }
    }
    configBuilder.setPassiveMode(options, true);
    configBuilder.setUserDirIsRoot(options, remoteConfig.userDirIsRoot);

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI.toString());
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
  public void verifyAndReconnect() throws StageException {
    boolean done = false;
    int retryCounter = 0;
    boolean reconnect = false;
    while (!done && retryCounter < MAX_RETRIES) {
      try {
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
      String file,
      String fileConfigName,
      CredentialValue password,
      String passwordConfigName,
      KeyStoreType keyStoreType,
      boolean isKeystore, // or truststore
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    if (file != null && !file.isEmpty()) {
      File keystoreFile = new File(file);
      String keystorePassword = resolveCredential(
          password,
          passwordConfigName,
          issues,
          context,
          group
      );
      try {
        KeyStore keystore = loadKeystore(
            keystoreFile,
            keyStoreType.getJavaValue(),
            keystorePassword
        );
          try {
            if (isKeystore) {
              FtpsFileSystemConfigBuilder.getInstance().setKeyManager(
                  options,
                  KeyManagerUtils.createClientKeyManager(keystore, null, keystorePassword)
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
                isKeystore ? "key" : "trust",
                e.getMessage(),
                e
            ));
          }
      } catch (IOException | GeneralSecurityException e) {
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
      if (isKeystore) {
        issues.add(context.createConfigIssue(group.getLabel(), fileConfigName, Errors.REMOTE_12));
      } else {
        issues.add(context.createConfigIssue(group.getLabel(), fileConfigName, Errors.REMOTE_13));
      }
    }
  }

  private KeyStore loadKeystore(File file, String type, String password)
      throws IOException, GeneralSecurityException {
    KeyStore keystore = KeyStore.getInstance(type);
    char[] passwordArr = password != null ? password.toCharArray() : null;
    try (FileInputStream fin = new FileInputStream(file)) {
      keystore.load(fin, passwordArr);
    }
    return keystore;
  }
}
