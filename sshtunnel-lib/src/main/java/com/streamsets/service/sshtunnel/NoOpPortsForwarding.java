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

import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * No-Op implementation used when SSH tunneling is not enabled.
 * <p/>
 * The port mapping returns the same target host port pair.
 */
public class NoOpPortsForwarding implements PortsForwarding {
  private final Map<SshTunnelService.HostPort, SshTunnelService.HostPort> mapping;

  public NoOpPortsForwarding(List<SshTunnelService.HostPort> targetHostsPorts) {
    mapping = targetHostsPorts.stream().collect(Collectors.toMap(e -> e, e -> e));
  }

  @Override
  public Map<SshTunnelService.HostPort, SshTunnelService.HostPort> getPortMapping() {
    return mapping;
  }

}
