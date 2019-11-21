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
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ServiceDef(
    provides = SshTunnelService.class,
    version = 1,
    upgraderDef = "sshtunnel-upgrader.yaml",
    label = "SSH Tunnel"
)
@ConfigGroups(Groups.class)
public class SshTunnelServiceImpl extends BaseService implements SshTunnelService {

  private static final Logger LOG = LoggerFactory.getLogger(SshTunnelServiceImpl.class);

  @ConfigDefBean(groups = {"SSH_TUNNEL"})
  public SshTunnelConfigBean sshTunnelConfig;

  @Override
  public List<ConfigIssue> init() {
    LOG.debug("Initializing SSH Tunnel service");

    List issues = new ArrayList();

    return issues;
  }

  @Override
  public void destroy() {
    LOG.debug("Destroying SSH Tunnel service");
  }


  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public PortsForwarding start(List<HostPort> targetHostsPorts) {
    return new PortsForwarding() {
      @Override
      public boolean isEnabled() {
        return false;
      }

      @Override
      public Map<HostPort, HostPort> getPortMapping() {
        return targetHostsPorts.stream().collect(Collectors.toMap(e -> e, e -> e));
      }

      @Override
      public void healthCheck() throws StageException {
      }

    };
  }

  @Override
  public void stop(PortsForwarding PortsForwarding) {

  }
}
