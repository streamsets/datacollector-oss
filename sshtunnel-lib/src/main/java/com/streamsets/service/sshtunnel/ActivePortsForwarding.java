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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation used when SSH tunneling is enabled.
 * <p/>
 * The returned host and port pairs are the entry points in the tunnel.
 */
public class ActivePortsForwarding implements LifecyclePortsForwarding {
  private static final Logger LOG = LoggerFactory.getLogger(ActivePortsForwarding.class);

  private final SshTunnel.Builder sshTunnelBuilder;
  private final List<SshTunnelService.HostPort> targetHostsPorts;
  private Map<SshTunnelService.HostPort, SshTunnelService.HostPort> mapping;
  private List<SshTunnel> sshTunnels;

  public ActivePortsForwarding(SshTunnel.Builder sshTunnelBuilder, List<SshTunnelService.HostPort> targetHostsPorts) {
    this.sshTunnelBuilder = Utils.checkNotNull(sshTunnelBuilder, "sshTunnelBuilder");
    this.targetHostsPorts = Utils.checkNotNull(targetHostsPorts, "targetHostsPorts");
    Utils.checkArgument(targetHostsPorts.size() > 0, "targetHostPorts must have at least one element");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  List<SshTunnel> getSshTunnels() {
    Utils.checkState(mapping != null && !mapping.isEmpty(), "Not running");
    return sshTunnels;
  }

  @Override
  public Map<SshTunnelService.HostPort, SshTunnelService.HostPort> getPortMapping() {
    Utils.checkState(mapping != null && !mapping.isEmpty(), "Not running");
    return mapping;
  }

  @Override
  public void start() {
    Utils.checkState(mapping == null, "Already Running");
    LOG.debug("Starting tunnels");
    sshTunnels = new ArrayList<>();
    Map<SshTunnelService.HostPort, SshTunnelService.HostPort> newMapping = new HashMap<>();
    try {
      for (SshTunnelService.HostPort targetHostPort : targetHostsPorts) {
        SshTunnel sshTunnel = sshTunnelBuilder.build();
        sshTunnel.start(targetHostPort.getHost(), targetHostPort.getPort());
        SshTunnelService.HostPort tunnelEntryHostPort = new SshTunnelService.HostPort(
            sshTunnel.getTunnelEntryHost(),
            sshTunnel.getTunnelEntryPort()
        );
        sshTunnels.add(sshTunnel);
        newMapping.put(targetHostPort, tunnelEntryHostPort);
      }
    } catch (StageException ex) {
      stop();
      sshTunnels.clear();
      throw ex;
    }
    this.mapping = Collections.unmodifiableMap(newMapping);
    LOG.debug("Started tunnels");
  }

  @Override
  public void stop() {
    Utils.checkState(mapping != null && !mapping.isEmpty(), "Not running");
    mapping = Collections.emptyMap();
    if (!sshTunnels.isEmpty()) {
      LOG.debug("Stopping tunnels");
      for (SshTunnel sshTunnel : sshTunnels) {
        try {
          LOG.debug("Stopping tunnel '{}:{}'", sshTunnel.getTunnelEntryHost(), sshTunnel.getTunnelEntryPort());
          sshTunnel.stop();
        } catch (Exception ex) {
          LOG.warn("Error stopping tunnel: {}", ex, ex);
        }
      }
      sshTunnels.clear();
      LOG.debug("Stopped tunnels");
    }
  }

  @Override
  public void healthCheck() throws StageException {
    Utils.checkState(mapping != null && !mapping.isEmpty(), "Not running");
    for (SshTunnel sshTunnel : sshTunnels) {
      sshTunnel.healthCheck();
    }
  }

}
