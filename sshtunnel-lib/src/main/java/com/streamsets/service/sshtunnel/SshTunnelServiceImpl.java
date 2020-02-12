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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SSH Tunnel service implementation.
 * <p/>
 * Provides embedded SSH tunnel entry points (as many as needed) for the stage using the service.
 */
@ServiceDef(
    provides = SshTunnelService.class,
    version = 1,
    upgraderDef = "sshtunnel-upgrader.yaml",
    label = "SSH Tunnel"
)
@ConfigGroups(Groups.class)
public class SshTunnelServiceImpl extends BaseService implements SshTunnelService {
  private static final Logger LOG = LoggerFactory.getLogger(SshTunnelServiceImpl.class);

  private int state; // 0 created, 1 inited, 2 error, 3 started, 4 stopped, 5 destroyed

  @ConfigDefBean(groups = {"SSH_TUNNEL"})
  public SshTunnelConfigBean sshTunnelConfig;

  private SshTunnel.Builder sshTunnelBuilder;

  private PortsForwarding portsForwarding;

  //visible for testing
  SshTunnel.Builder createSshTunnelBuilder() {
    return SshTunnel.builder();
  }

  @Override
  public List<ConfigIssue> init() {
    Utils.checkState(state == 0, "Already inited");
    LOG.debug("Initializing SSH Tunnel service");

    List issues = new ArrayList();

    if (sshTunnelConfig.sshTunneling) {
      SshKeyInfoBean sshKeyInfo = sshTunnelConfig.getSshKeyInfo();
      if (sshKeyInfo != null) {
        //splitting the fingerprints config into one a List<String> of fingerprints
        //didn't feel like bringing in Guava just for this.
        List<String> fingerprints = Arrays.stream(sshTunnelConfig.sshHostFingerprints.split(","))
            .map(String::trim)
            .filter(e -> !e.isEmpty())
            .collect(Collectors.toList());

        sshTunnelBuilder = createSshTunnelBuilder()
            .setSshHost(sshTunnelConfig.sshHost)
            .setSshPort(sshTunnelConfig.sshPort)
            .setSshUser(sshTunnelConfig.sshUsername)
            .setSshHostFingerprints(fingerprints)
            .setSshPrivateKey(sshKeyInfo.getPrivateKey())
            .setSshPublicKey(sshKeyInfo.getPublicKey())
            .setSshPrivateKeyPassword(sshKeyInfo.getPassword())
            .setReadyTimeOut(sshTunnelConfig.sshReadyTimeout)
            .setSshKeepAlive(sshTunnelConfig.sshKeepAlive)
        ;
        try {
          // create a dummy tunnel to verify required config values are set. It does not check correctness of data here.
          sshTunnelBuilder.build();
        } catch (Exception ex) {
          issues.add(getContext().createConfigIssue(
              Groups.SSH_TUNNEL.name(),
              "",
              Errors.SSH_TUNNEL_03,
              ex.getMessage()
          ));
        }
      }
    }
    state = (issues.isEmpty()) ? 1 : 2;
    return issues;
  }

  @Override
  public void destroy() {
    LOG.debug("Destroying SSH Tunnel service");
    if (state == 3) {
      stop();
    }
    state = 5;
  }

  @Override
  public boolean isEnabled() {
    return sshTunnelConfig.sshTunneling;
  }

  @Override
  public Map<HostPort, HostPort> start(List<HostPort> targetHostsPorts) {
    Utils.checkState(state == 1, "Not in inited state");
    state = 3;
    if (sshTunnelBuilder == null) {
      LOG.debug("SSH tunneling disabled, creating a NoOpPortsForwarding");
      portsForwarding = new NoOpPortsForwarding(targetHostsPorts);
    } else {
      LOG.debug("SSH tunneling enabled, creating an ActivePortsForwarding");
      try {
        portsForwarding = new ActivePortsForwarding(sshTunnelBuilder.build(), targetHostsPorts);
      } catch (Exception ex) {
        state = 2;
        throw  ex;
      }
    }
    portsForwarding.start();
    state = 3;
    return portsForwarding.getPortMapping();
  }

  @Override
  public void healthCheck() throws StageException {
    Utils.checkState(state == 3, "Not in started state");
    portsForwarding.healthCheck();
  }

  @Override
  public void stop() {
    Utils.checkState(state == 3, "Not in started state");
    state = 4;
    portsForwarding.stop();
  }

}
