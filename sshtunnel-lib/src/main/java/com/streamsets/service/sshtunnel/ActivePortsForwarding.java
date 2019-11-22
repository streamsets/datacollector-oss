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

import java.util.List;
import java.util.Map;

/**
 * Implementation used when SSH tunneling is enabled.
 * <p/>
 * The returned host and port pairs are the entry points in the tunnel.
 */
public class ActivePortsForwarding implements PortsForwarding {
  private static final Logger LOG = LoggerFactory.getLogger(ActivePortsForwarding.class);

  private volatile int state; //0 created, 1 started, 2 stopped
  private final SshTunnel sshTunnel;
  private final List<SshTunnelService.HostPort> targetHostsPorts;
  private Map<SshTunnelService.HostPort, SshTunnelService.HostPort> mapping;

  public ActivePortsForwarding(SshTunnel sshTunnel, List<SshTunnelService.HostPort> targetHostsPorts) {
    this.sshTunnel = Utils.checkNotNull(sshTunnel, "sshTunnel");
    this.targetHostsPorts = Utils.checkNotNull(targetHostsPorts, "targetHostsPorts");
    Utils.checkArgument(targetHostsPorts.size() > 0, "targetHostPorts must have at least one element");
  }

  @Override
  public Map<SshTunnelService.HostPort, SshTunnelService.HostPort> getPortMapping() {
    Utils.checkState(state == 1, "Not running");
    return mapping;
  }

  @Override
  public synchronized void start() {
    Utils.checkState(state == 0, "Already started");
    LOG.debug("Starting tunnel and port forwarders");
    try {
      mapping = sshTunnel.start(targetHostsPorts);
      state = 1;
    } catch (Exception ex) {
      state = 2;
      throw ex;
    }
    LOG.debug("Started tunnel and port forwarders");
  }

  @Override
  public synchronized void stop() {
    Utils.checkState(state == 1, "Not running");
    state = 2;
    LOG.debug("Stopping tunnels");
    sshTunnel.stop();
    LOG.debug("Stopped tunnels");
  }

  @Override
  public void healthCheck() throws StageException {
    Utils.checkState(state == 1, "Not running");
    sshTunnel.healthCheck();
  }

}
