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
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.compression.DelayedZlibCompression;
import net.schmizz.sshj.transport.compression.NoneCompression;
import net.schmizz.sshj.transport.compression.ZlibCompression;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.userauth.password.PasswordUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles all of the logic about connecting to an SFTP server, including config verification.  Subclasses can get
 * this functionality for free by simply calling {@link #initAndConnect(List, ConfigIssueContext, URI, Label, Label)}
 * and making sure {@link RemoteConfigBean} is setup properly.
 */
public abstract class SFTPRemoteConnector extends RemoteConnector {

  private static final Logger LOG = LoggerFactory.getLogger(SFTPRemoteConnector.class);
  private static final String SFTP_SCHEME = "sftp";
  public static final int DEFAULT_PORT = 22;
  private static final Pattern URL_PATTERN_SFTP = Pattern.compile("(sftp://).*:?.*");

  protected File knownHostsFile;
  protected SSHClient sshClient;
  protected ChrootSFTPClient sftpClient;
  protected SSHClientRebuilder sshClientRebuilder;

  protected SFTPRemoteConnector(RemoteConfigBean remoteConfig) {
    super(remoteConfig);
  }

  @Override
  public void initAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      URI remoteURI,
      Label remoteGroup,
      Label credGroup
  ) {
    if (remoteConfig.connection.credentials.knownHosts != null
        && !remoteConfig.connection.credentials.knownHosts.isEmpty()) {
      this.knownHostsFile = new File(remoteConfig.connection.credentials.knownHosts);
    } else {
      this.knownHostsFile = null;
    }

    sshClientRebuilder = new SSHClientRebuilder();
    DefaultConfig sshConfig = new DefaultConfig();
    sshConfig.setCompressionFactories(Arrays.asList(
        new DelayedZlibCompression.Factory(),
        new ZlibCompression.Factory(),
        new NoneCompression.Factory()));
    sshClient = new SSHClient(sshConfig);
    sshClientRebuilder.setConfig(sshConfig);

    if (remoteConfig.connection.credentials.strictHostChecking) {
      if (knownHostsFile != null) {
        if (knownHostsFile.exists() && knownHostsFile.isFile() && knownHostsFile.canRead()) {
          try {
            sshClient.loadKnownHosts(knownHostsFile);
            sshClientRebuilder.setKnownHosts(knownHostsFile);
          } catch (IOException ex) {
            issues.add(
                context.createConfigIssue(
                    credGroup.getLabel(),
                    CONF_PREFIX + "knownHosts",
                    Errors.REMOTE_03,
                    knownHostsFile
                ));
          }
        } else {
          issues.add(
              context.createConfigIssue(credGroup.getLabel(),
                  CONF_PREFIX + "knownHosts",
                  Errors.REMOTE_03,
                  knownHostsFile
              ));
        }
      } else {
        issues.add(
            context.createConfigIssue(credGroup.getLabel(),
                CONF_PREFIX + "strictHostChecking",
                Errors.REMOTE_04
            ));
      }
    } else {
      // Strict host checking off
      sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }

    String username = resolveUsername(remoteURI, issues, context, credGroup);
    sshClientRebuilder.setUsername(username);
    KeyProvider keyProvider = null;
    String password = null;
    try {
      switch (remoteConfig.connection.credentials.auth) {
        case PRIVATE_KEY:
          String privateKeyPassphrase = resolveCredential(
              remoteConfig.privateKeyPassphrase,
              CONF_PREFIX + "privateKeyPassphrase",
              issues,
              context,
              credGroup
          );
          switch (remoteConfig.privateKeyProvider) {
            case FILE:
              File privateKeyFile = new File(remoteConfig.privateKey);
              sshClientRebuilder.setKeyLocation(privateKeyFile.getAbsolutePath());
              if (!privateKeyFile.exists() || !privateKeyFile.isFile() || !privateKeyFile.canRead()) {
                issues.add(context.createConfigIssue(
                    credGroup.getLabel(),
                    CONF_PREFIX + "privateKey",
                    Errors.REMOTE_05,
                    remoteConfig.privateKey
                ));
              } else {
                if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
                  keyProvider = sshClient.loadKeys(privateKeyFile.getAbsolutePath(), privateKeyPassphrase);
                  sshClientRebuilder.setKeyPassphrase(privateKeyPassphrase);
                } else {
                  keyProvider = sshClient.loadKeys(privateKeyFile.getAbsolutePath());
                }
              }
              break;
            case PLAIN_TEXT:
              String privateKeyPlainText = resolveCredential(
                  remoteConfig.privateKeyPlainText,
                  CONF_PREFIX + "privateKeyPlainText",
                  issues,
                  context,
                  credGroup
              );
              sshClientRebuilder.setKeyPlainText(privateKeyPlainText);
              if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
                keyProvider = sshClient.loadKeys(
                    privateKeyPlainText,
                    null,
                    PasswordUtils.createOneOff(privateKeyPassphrase.toCharArray())
                );
                sshClientRebuilder.setKeyPassphrase(privateKeyPassphrase);
              } else {
                keyProvider = sshClient.loadKeys(privateKeyPlainText, null, null);
              }
              break;
            default:
              break;
          }
          // No need to bother trying this if the keyProvider is still null - issues were already found
          if (keyProvider != null) {
            // Verify we can get the private key
            keyProvider.getPrivate();
          }
          break;
        case PASSWORD:
          password = resolvePassword(remoteURI, issues, context, credGroup);
          sshClientRebuilder.setPassword(password);
          break;
        default:
          break;
      }
    } catch (IOException e) {
      LOG.error(Errors.REMOTE_10.getMessage(), e.getMessage(), e);
      issues.add(context.createConfigIssue(
          credGroup.getLabel(),
          CONF_PREFIX
              + (remoteConfig.privateKeyProvider.equals(PrivateKeyProvider.PLAIN_TEXT)
              ? "privateKeyPlainText" : "privateKey"),
          Errors.REMOTE_10,
          e.getMessage(),
          e
      ));
    }

    if (remoteConfig.connection.protocol == Protocol.SFTP
        && !URL_PATTERN_SFTP.matcher(remoteConfig.connection.remoteAddress).matches()) {
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
      LOG.info("Connecting to {}", remoteURI.toString());
      try {
        sshClient.connect(remoteURI.getHost(), remoteURI.getPort());
        sshClientRebuilder.setHostPort(remoteURI.getHost(), remoteURI.getPort());
        switch (remoteConfig.connection.credentials.auth) {
          case PRIVATE_KEY:
            sshClient.authPublickey(username, keyProvider);
            break;
          case PASSWORD:
            sshClient.authPassword(username, password);
            break;
          default:
            break;
        }
        String remotePath = remoteURI.getPath().trim();
        if (remotePath.isEmpty()) {
          remotePath = "/";
        }

        // SSHJ uses 0 to indicate no timeout
        sshClient.setTimeout(remoteConfig.socketTimeout * 1000);
        sshClientRebuilder.setSocketTimeout(sshClient.getTimeout());
        sshClient.setConnectTimeout(remoteConfig.connectionTimeout * 1000);
        sshClientRebuilder.setConnectionTimeout(sshClient.getConnectTimeout());
        SFTPClient sftpClientRaw = sshClient.newSFTPClient();
        sftpClientRaw.getSFTPEngine().setTimeoutMs(remoteConfig.dataTimeout * 1000);

        this.sftpClient = new ChrootSFTPClient(
            sftpClientRaw,
            remotePath,
            remoteConfig.userDirIsRoot,
            remoteConfig.createPathIfNotExists,
            remoteConfig.disableReadAheadStream
        );
        this.sftpClient.ls();
      } catch (IOException e) {
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
          sshClient = sshClientRebuilder.build();
          SFTPClient sftpClientRaw = sshClient.newSFTPClient();
          sftpClientRaw.getSFTPEngine().setTimeoutMs(remoteConfig.dataTimeout * 1000);
          sftpClient.setSFTPClient(sftpClientRaw);
          reconnect = false;
        }
        sftpClient.ls();
        done = true;
      } catch (IOException ioe) {
        // ls can fail due to session is down, a timeout, etc; so try getting a new connection
        if (retryCounter < MAX_RETRIES - 1) {
          LOG.info("Got IOException when trying to ls remote directory. '{}'", ioe.getMessage(), ioe);
          LOG.warn("Retrying connection to remote directory");
          reconnect = true;
        } else {
          throw new StageException(Errors.REMOTE_09, ioe.getMessage(), ioe);
        }
      }
      retryCounter++;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (sftpClient != null) {
        sftpClient.close();
      }
    } finally {
      if (sshClient != null) {
        sshClient.close();
      }
    }
  }

  protected static String slashify(String file) {
    // For backwards compatibility/consistency we need to add a leading /
    return StringUtils.prependIfMissing(file, "/");
  }
}
