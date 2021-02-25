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
import com.streamsets.pipeline.stage.connection.remote.Protocol;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Standardizes the way we connect to a remote source/target (SFTP or FTP) and provides some commons helper methods.
 */
public abstract class RemoteConnector {
  protected static final String CONF_PREFIX = "conf.remoteConfig.";
  protected static final int MAX_RETRIES = 2;
  private static final String USER_INFO_SEPARATOR = ":";

  protected RemoteConfigBean remoteConfig;

  protected RemoteConnector(RemoteConfigBean remoteConfig) {
    this.remoteConfig = remoteConfig;
  }

  public static URI getURI(
      RemoteConfigBean config,
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      Label group
  ) {
    String uriString = config.connection.remoteAddress;

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
      if (config.connection.protocol == Protocol.FTP || config.connection.protocol == Protocol.FTPS) {
        if (uri.getPort() == -1) {
          uri = UriBuilder.fromUri(uri).port(FTPRemoteConnector.DEFAULT_PORT).build();
        }
      } else if (config.connection.protocol == Protocol.SFTP) {
        if (uri.getPort() == -1) {
          uri = UriBuilder.fromUri(uri).port(SFTPRemoteConnector.DEFAULT_PORT).build();
        }
      } else {
        issues.add(context.createConfigIssue(
            group.getLabel(),
            CONF_PREFIX + "remoteAddress",
            Errors.REMOTE_02,
            uriString
        ));
        uri = null;
      }
    } catch (URISyntaxException ex) {
      issues.add(context.createConfigIssue(
          group.getLabel(),
          CONF_PREFIX + "remoteAddress",
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
    return resolveCredential(remoteConfig.connection.credentials.username, CONF_PREFIX + "username", issues, context, group);
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
    return resolveCredential(remoteConfig.connection.credentials.password, CONF_PREFIX + "password", issues, context, group);
  }

  protected abstract void initAndConnect(
      List<Stage.ConfigIssue> issues,
      ConfigIssueContext context,
      URI remoteURI,
      Label remoteGroup,
      Label credGroup
  );

  protected abstract void verifyAndReconnect() throws StageException;

  public abstract void close() throws IOException;
}
