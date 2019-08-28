/*
 * Copyright 2018 StreamSets Inc.
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

import net.schmizz.sshj.Config;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.userauth.password.PasswordUtils;

import java.io.File;
import java.io.IOException;

/**
 * Stores the basic configs for creating an {@link SSHClient} to allow the creation of duplicate clients. Should be
 * used by {@link SFTPRemoteConnector}, which does all of the init error checking and handling.
 */
class SSHClientRebuilder {

  private Config config;
  private File knownHosts;
  private String keyLocation;
  private String keyPlainText;
  private String keyPassphrase;
  private String hostname;
  private int port;
  private String username;
  private String password;
  private int socketTimeout;
  private int connectionTimeout;

  public SSHClient build() throws IOException {
    SSHClient sshClient = new SSHClient(config);
    if (knownHosts != null) {
      sshClient.loadKnownHosts(knownHosts);
    } else {
      sshClient.addHostKeyVerifier(new PromiscuousVerifier());
    }
    KeyProvider keyProvider = null;
    if (password == null) {
      if (keyLocation != null) {
        if (keyPassphrase != null) {
          keyProvider = sshClient.loadKeys(keyLocation, keyPassphrase);
        } else {
          keyProvider = sshClient.loadKeys(keyLocation);
        }
      } else {
        if (keyPassphrase != null) {
          keyProvider = sshClient.loadKeys(keyPlainText, null, PasswordUtils.createOneOff(keyPassphrase.toCharArray()));
        } else {
          keyProvider = sshClient.loadKeys(keyPlainText, null, null);
        }
      }
    }
    sshClient.connect(hostname, port);
    if (password == null) {
      sshClient.authPublickey(username, keyProvider);
    } else {
      sshClient.authPassword(username, password);
    }
    sshClient.setTimeout(socketTimeout * 1000);
    sshClient.setConnectTimeout(connectionTimeout * 1000);
    return sshClient;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  public void setKnownHosts(File knownHosts) {
    this.knownHosts = knownHosts;
  }

  public void setKeyLocation(String keyLocation) {
    this.keyLocation = keyLocation;
  }

  public void setKeyPlainText(String keyPlainText) {
    this.keyPlainText = keyPlainText;
  }

  public void setKeyPassphrase(String keyPassphrase) {
    this.keyPassphrase = keyPassphrase;
  }

  public void setHostPort(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public void setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }
}
