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
package com.streamsets.service.sshtunnel;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.credential.CredentialValue;

@ConfigGroups(Groups.class)
public class SshTunnelConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "SSH Tunneling",
      description = "Uses SSH tunneling to securely connect to a system through an SSH server",
      displayPosition = 1000,
      group = "SSH_TUNNEL"
  )
  public boolean sshTunneling;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependencies = {@Dependency(configName = "sshTunneling", triggeredByValues = "true")},
      label = "SSH Tunnel Host",
      description = "Host name for the SSH server",
      displayPosition = 1010,
      group = "SSH_TUNNEL"
  )
  public String sshHost;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "22",
      dependencies = {@Dependency(configName = "sshTunneling", triggeredByValues = "true")},
      label = "SSH Tunnel Port",
      description = "Port number for the SSH server",
      displayPosition = 1020,
      group = "SSH_TUNNEL",
      min = 1,
      max = 65535
  )
  public int sshPort;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      dependencies = {@Dependency(configName = "sshTunneling", triggeredByValues = "true")},
      label = "SSH Tunnel Host Fingerprint",
      description = "Host key fingerprint used to verify the identity of the SSH server, such as " +
          "'SHA256:Ab1CdeFg+hIJk2L682MNopOQrSTuv3wxYz3ABcdeFGH'. Multiple fingerprints can be " +
          "separated by commas. Be sure to read the documentation for instructions on properly " +
          "generating these fingerprints. Leave blank to establish an SSH connection without any verification.",
      displayPosition = 1030,
      group = "SSH_TUNNEL"
  )
  public String sshHostFingerprints;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependencies = {@Dependency(configName = "sshTunneling", triggeredByValues = "true")},
      label = "SSH Tunnel Username",
      displayPosition = 1040,
      group = "SSH_TUNNEL"
  )
  public String sshUsername;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      upload = ConfigDef.Upload.TEXT,
      defaultValue = "${credential:get('streamsets', 'all','sdc/defaultPrivateKey')}",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true")
      },
      label = "SSH Private Key",
      description = "To generate the SSH keys, on a terminal session, run " +
          "\"ssh-keygen -f x -m PEM -t rsa -b 4096\". A password must be specified",
      displayPosition = 1050,
      group = "SSH_TUNNEL"
  )
  public CredentialValue sshPrivateKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "${credential:get('streamsets', 'all','sdc/defaultPrivateKeyPassword')}",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true")
      },
      label = "Password of the SSH private key",
      description = "The SSH private key must have a password",
      displayPosition = 1060,
      group = "SSH_TUNNEL"
  )
  public CredentialValue sshPrivateKeyPassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      upload = ConfigDef.Upload.TEXT,
      defaultValue = "${credential:get('streamsets', 'all','sdc/defaultPublicKey')}",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true")
      },
      label = "SSH Public Key",
      displayPosition = 1070,
      group = "SSH_TUNNEL"
  )
  public CredentialValue sshPublicKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true"),
      },
      label = "SSH Tunnel Ready Timeout (milli-secs)",
      description = "Time to wait for the tunnel to be established before a pipeline error occurs",
      displayPosition = 1080,
      group = "SSH_TUNNEL",
      min = 1
  )
  public int sshReadyTimeout;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "30",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true"),
      },
      label = "SSH Tunnel Keep Alive (secs)",
      description = "Send a keep alive signal on this interval to prevent the SSH server from terminating " +
          "the connection if it is otherwise idle. Set to 0 to disable it.",
      displayPosition = 1090,
      group = "SSH_TUNNEL",
      min = 0
  )
  public int sshKeepAlive;

  SshKeyInfoBean getSshKeyInfo() {
    return new SshKeyInfoBean()
        .setPrivateKey(sshPrivateKey.get())
        .setPassword(sshPrivateKeyPassword.get())
        .setPublicKey(sshPublicKey.get());
  }

}
