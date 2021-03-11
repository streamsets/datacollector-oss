/*
 * Copyright 2021 StreamSets Inc.
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
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigIssueContext;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.stage.connection.remote.FTPSTrustStore;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnectionGroups;
import com.streamsets.pipeline.stage.destination.remote.Groups;
import com.streamsets.pipeline.stage.destination.remote.RemoteUploadTargetDelegate;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.compression.DelayedZlibCompression;
import net.schmizz.sshj.transport.compression.NoneCompression;
import net.schmizz.sshj.transport.compression.ZlibCompression;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.userauth.password.PasswordUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@StageDef(
    version = 1,
    label = "SFTP/FTP/FTPS Connection Verifier",
    description = "Verifies connection to SFTP/FTP/FTPS",
    upgraderDef = "upgrader/RemoteConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(RemoteConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = RemoteConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class RemoteConnectionVerifier extends ConnectionVerifier {

  private final static Logger LOG = LoggerFactory.getLogger(RemoteConnection.class);
  protected FileSystemOptions options;
  protected FileObject remoteDir;
  protected File knownHostsFile;
  protected SSHClient sshClient;
  protected ChrootSFTPClient sftpClient;
  protected SSHClientRebuilder sshClientRebuilder;

  private static final String FTP_SCHEME = "ftp";
  private static final String FTPS_SCHEME = "ftps";
  private static final Groups REMOTE_GROUP = Groups.REMOTE;
  private static final Groups CRED_GROUP = Groups.CREDENTIALS;
  private static final String CONN_PREFIX = "connection.";
  private static final String CONN_CRED_PREFIX = CONN_PREFIX + "credentials.";
  private static final String USER_INFO_SEPARATOR = ":";
  private static final Pattern URL_PATTERN_FTP = Pattern.compile("(ftp://).*:?.*");
  private static final Pattern URL_PATTERN_FTPS = Pattern.compile("(ftps://).*:?.*");
  private static final Pattern URL_PATTERN_SFTP = Pattern.compile("(sftp://).*:?.*");

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = RemoteConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public RemoteConnection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (connection.protocol == Protocol.SFTP) {
      SFTPinitAndConnect(issues, getContext(), connection);
    } else {
      FTPinitAndConnect(issues, getContext(), connection);
    }
    return issues;
  }

  protected void FTPinitAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      RemoteConnection connection
  ) {
    LOG.info("Starting connection to remote server");
    options = new FileSystemOptions();
    URI remoteURI = getURI(connection, issues, getContext(), REMOTE_GROUP);

    switch (connection.credentials.auth) {
      case PRIVATE_KEY:
        issues.add(context.createConfigIssue(CRED_GROUP.getLabel(), CONN_PREFIX + "privateKey", Errors.REMOTE_06));
        break;
      case PASSWORD:
        String username = resolveUsername(remoteURI, issues, context, CRED_GROUP);
        String password = resolvePassword(remoteURI, issues, context, CRED_GROUP);
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
        issues.add(context.createConfigIssue(CRED_GROUP.getLabel(), CONN_CRED_PREFIX + "auth", Errors.REMOTE_07,
            connection.credentials.auth.getLabel()));
        break;
    }

    if (remoteURI.getScheme() != null) {
      if (remoteURI.getScheme().equals(FTPS_SCHEME)) {

        FtpsFileSystemConfigBuilder.getInstance().setFtpsMode(options, connection.ftpsMode.getMode());
        FtpsFileSystemConfigBuilder.getInstance().setDataChannelProtectionLevel(options,
            connection.ftpsDataChannelProtectionLevel.getLevel()
        );
      }
    } else {
      issues.add(
          context.createConfigIssue(
              CRED_GROUP.getLabel(),
              CONN_PREFIX + "remoteAddress",
              Errors.REMOTE_18,
              connection.remoteAddress
          )
      );
    }

    if (connection.protocol == Protocol.FTP && !URL_PATTERN_FTP.matcher(connection.remoteAddress).matches()
        || connection.protocol == Protocol.FTPS && !URL_PATTERN_FTPS.matcher(connection.remoteAddress).matches()) {
      issues.add(
          context.createConfigIssue(
              CRED_GROUP.getLabel(),
              CONN_PREFIX + "remoteAddress",
              Errors.REMOTE_17,
              connection.remoteAddress,
              connection.protocol
          )
      );
    }

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI);
      try {
        remoteDir = VFS.getManager().resolveFile(remoteURI.toString(), options);
        remoteDir.refresh();
        remoteDir.createFolder();
        remoteDir.getChildren();
      } catch (FileSystemException e) {
        LOG.error(Errors.REMOTE_11.getMessage(), remoteURI.toString(), e.getMessage(), e);
        issues.add(context.createConfigIssue(
            REMOTE_GROUP.getLabel(),
            CONN_PREFIX + "remoteAddress",
            Errors.REMOTE_11,
            remoteURI.toString(),
            e.getMessage(),
            e
        ));
      }
    }
  }

  protected void SFTPinitAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      RemoteConnection connection
  ) {
    LOG.info("Starting connection to remote server");
    options = new FileSystemOptions();
    URI remoteURI = getURI(connection, issues, getContext(), REMOTE_GROUP);

    if (connection.credentials.knownHosts != null
        && !connection.credentials.knownHosts.isEmpty()) {
      this.knownHostsFile = new File(connection.credentials.knownHosts);
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

    if (connection.credentials.strictHostChecking) {
      if (knownHostsFile != null) {
        if (knownHostsFile.exists() && knownHostsFile.isFile() && knownHostsFile.canRead()) {
          try {
            sshClient.loadKnownHosts(knownHostsFile);
            sshClientRebuilder.setKnownHosts(knownHostsFile);
          } catch (IOException ex) {
            issues.add(
                context.createConfigIssue(
                    CRED_GROUP.getLabel(),
                    CONN_PREFIX + "knownHosts",
                    Errors.REMOTE_03,
                    knownHostsFile
                ));
          }
        } else {
          issues.add(
              context.createConfigIssue(CRED_GROUP.getLabel(),
                  CONN_PREFIX + "knownHosts",
                  Errors.REMOTE_03,
                  knownHostsFile
              ));
        }
      } else {
        issues.add(
            context.createConfigIssue(CRED_GROUP.getLabel(),
                CONN_PREFIX + "strictHostChecking",
                Errors.REMOTE_04
            ));
      }
    } else {
      // Strict host checking off
      sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }

    String username = resolveUsername(remoteURI, issues, context, CRED_GROUP);
    sshClientRebuilder.setUsername(username);
    KeyProvider keyProvider = null;
    String password = null;
    try {
      switch (connection.credentials.auth) {
        case PRIVATE_KEY:
          // No need to bother trying this if the keyProvider is still null - issues were already found
          if (keyProvider != null) {
            // Verify we can get the private key
            keyProvider.getPrivate();
          }
          break;
        case PASSWORD:
          password = resolvePassword(remoteURI, issues, context, CRED_GROUP);
          sshClientRebuilder.setPassword(password);
          break;
        default:
          break;
      }
    } catch (IOException e) {
      LOG.error(Errors.REMOTE_10.getMessage(), e.getMessage(), e);
      issues.add(context.createConfigIssue(
          CRED_GROUP.getLabel(),
          CONN_PREFIX + "privateKey",
          Errors.REMOTE_10,
          e.getMessage(),
          e
      ));
    }

    if (connection.protocol == Protocol.SFTP
        && !URL_PATTERN_SFTP.matcher(connection.remoteAddress).matches()) {
      issues.add(
          context.createConfigIssue(
              CRED_GROUP.getLabel(),
              CONN_PREFIX + "remoteAddress",
              Errors.REMOTE_17,
              connection.remoteAddress,
              connection.protocol
          )
      );
    }

    // Only actually try to connect and authenticate if there were no issues
    if (issues.isEmpty()) {
      LOG.info("Connecting to {}", remoteURI.toString());
      try {
        sshClient.connect(remoteURI.getHost(), remoteURI.getPort());
        sshClientRebuilder.setHostPort(remoteURI.getHost(), remoteURI.getPort());
        switch (connection.credentials.auth) {
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
        sshClientRebuilder.setConnectionTimeout(sshClient.getConnectTimeout());
        SFTPClient sftpClientRaw = sshClient.newSFTPClient();

        this.sftpClient = new ChrootSFTPClient(
            sftpClientRaw,
            remotePath,
            false,
            true,
            true
        );
        this.sftpClient.ls();
      } catch (IOException e) {
        LOG.error(Errors.REMOTE_11.getMessage(), remoteURI.toString(), e.getMessage(), e);
        issues.add(context.createConfigIssue(
            REMOTE_GROUP.getLabel(),
            CONN_PREFIX + "remoteAddress",
            Errors.REMOTE_11,
            remoteURI.toString(),
            e.getMessage(),
            e
        ));
      }
    }
  }

  protected static URI getURI(
      RemoteConnection connection,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    String uriString = connection.remoteAddress;

    // This code handles the case where the username or password, if given in the URI, contains an '@' character.  We
    // need to percent-encode it (i.e. "%40") or else the URI parsing will appear to succeed but be done incorrectly
    // because '@' is also used as the delimiter for userinfo and host.  This code basically just replaces all but
    // the last '@' with "%40".
    StringBuilder sb = new StringBuilder();
    boolean doneFirstAt = false;
    for (int i = uriString.length() - 1; i >= 0; i--) {
      char c = uriString.charAt(i);
      if (c == '@') {
        if (doneFirstAt) {
          sb.insert(0, "%40");
        } else {
          sb.insert(0, '@');
          doneFirstAt = true;
        }
      } else {
        sb.insert(0, c);
      }
    }
    uriString = sb.toString();

    URI uri;
    try {
      uri = new URI(uriString);
      if (connection.protocol == Protocol.FTP || connection.protocol == Protocol.FTPS) {
        if (uri.getPort() == -1) {
          uri = UriBuilder.fromUri(uri).port(FTPRemoteConnector.DEFAULT_PORT).build();
        }
      } else if (connection.protocol == Protocol.SFTP) {
        if (uri.getPort() == -1) {
          uri = UriBuilder.fromUri(uri).port(SFTPRemoteConnector.DEFAULT_PORT).build();
        }
      } else {
        issues.add(context.createConfigIssue(
            group.getLabel(),
            CONN_PREFIX + "remoteAddress",
            Errors.REMOTE_02,
            uriString
        ));
        uri = null;
      }
    } catch (URISyntaxException ex) {
      issues.add(context.createConfigIssue(
          group.getLabel(),
          CONN_PREFIX + "remoteAddress",
          Errors.REMOTE_01,
          uriString
      ));
      uri = null;
    }
    return uri;
  }

  protected String resolveCredential(
      CredentialValue credentialValue,
      String config,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      issues.add(context.createConfigIssue(
          group.getLabel(),
          config,
          Errors.REMOTE_08,
          e.toString()
      ));
    }
    return null;
  }

  protected String resolveUsername(
      URI remoteURI,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    String userInfo = remoteURI.getUserInfo();
    if (userInfo != null) {
      if (userInfo.contains(USER_INFO_SEPARATOR)) {
        return userInfo.substring(0, userInfo.indexOf(USER_INFO_SEPARATOR));
      }
      return userInfo;
    }
    return resolveCredential(connection.credentials.username, CONN_CRED_PREFIX + "username", issues, context, group);
  }

  protected String resolvePassword(
      URI remoteURI,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    String userInfo = remoteURI.getUserInfo();
    if (userInfo != null && userInfo.contains(USER_INFO_SEPARATOR)) {
      return userInfo.substring(userInfo.indexOf(USER_INFO_SEPARATOR) + 1);
    }
    return resolveCredential(connection.credentials.password, CONN_CRED_PREFIX + "password", issues, context, group);
  }

}
