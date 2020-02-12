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
      displayPosition = 1000,
      group = "SSH_TUNNEL"
  )
  public boolean sshTunneling;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependencies = {@Dependency(configName = "sshTunneling", triggeredByValues = "true")},
      label = "SSH Tunnel Host",
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
      description = "To obtain the fingerprint, on a terminal session of the SSH server, run " +
          "\"for i in /etc/ssh/*.pub; do ssh-keygen -lf $i|awk '{print $2\",\"}'; done;\"",
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
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true")
      },
      label = "SSH Private Key Password",
      description = "The SSH private key cannot be passwordless",
      displayPosition = 1060,
      group = "SSH_TUNNEL"
  )
  public CredentialValue sshPrivateKeyPassword;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true")
      },
      label = "SSH Public Key",
      displayPosition = 1070,
      group = "SSH_TUNNEL"
  )
  public String sshPublicKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      dependencies = {
          @Dependency(configName = "sshTunneling", triggeredByValues = "true"),
      },
      label = "SSH Tunnel Ready Timeout (milli-secs)",
      description = "Time to wait for tunnel to establish",
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
      description = "Set it to 0 to disable it",
      displayPosition = 1090,
      group = "SSH_TUNNEL",
      min = 0
  )
  public int sshKeepAlive;

  SshKeyInfoBean getSshKeyInfo() {
    return new SshKeyInfoBean()
        .setPrivateKey(sshPrivateKey.get())
        .setPassword(sshPrivateKeyPassword.get())
        .setPublicKey(sshPublicKey);
  }

}
