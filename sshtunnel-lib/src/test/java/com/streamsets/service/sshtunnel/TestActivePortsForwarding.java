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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestActivePortsForwarding {

  @Test
  public void testTunnelsLifecycle() {
    SshTunnelService.HostPort targetHostPort = new SshTunnelService.HostPort("l", 1);

    SshTunnelService.HostPort forwarderHostPort = new SshTunnelService.HostPort("L", 2);

    SshTunnel tunnel = Mockito.mock(SshTunnel.class);
    Mockito.when(tunnel.start(Mockito.eq(ImmutableList.of(targetHostPort)))).thenReturn(ImmutableMap.of(
        targetHostPort,
        forwarderHostPort
    ));

    SshTunnel.Builder tunnelBuilder = Mockito.mock(SshTunnel.Builder.class);
    Mockito.when(tunnelBuilder.build()).thenReturn(tunnel);

    ActivePortsForwarding portsForwarding = new ActivePortsForwarding(tunnel, ImmutableList.of(targetHostPort));

    portsForwarding.start();

    Mockito.verify(tunnel, Mockito.times(1)).start(Mockito.eq(ImmutableList.of(targetHostPort)));

    Assert.assertEquals(ImmutableMap.of(targetHostPort, forwarderHostPort), portsForwarding.getPortMapping());

    Mockito.verify(tunnel, Mockito.times(0)).healthCheck();
    portsForwarding.healthCheck();
    Mockito.verify(tunnel, Mockito.times(1)).healthCheck();

    Mockito.verify(tunnel, Mockito.times(0)).stop();
    portsForwarding.stop();
    Mockito.verify(tunnel, Mockito.times(1)).stop();

  }

  @Test
  public void testTunnelsIllegalStates() {
    SshTunnelService.HostPort targetHostPort = new SshTunnelService.HostPort("l", 1);

    SshTunnelService.HostPort forwarderHostPort = new SshTunnelService.HostPort("L", 2);

    SshTunnel tunnel = Mockito.mock(SshTunnel.class);
    Mockito.when(tunnel.start(Mockito.eq(ImmutableList.of(targetHostPort)))).thenReturn(ImmutableMap.of(
        targetHostPort,
        forwarderHostPort
    ));


    ActivePortsForwarding portsForwarding = new ActivePortsForwarding(tunnel, ImmutableList.of(targetHostPort));

    try {
      portsForwarding.stop();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    try {
      portsForwarding.getPortMapping();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    portsForwarding.start();

    try {
      portsForwarding.start();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    portsForwarding.stop();

    try {
      portsForwarding.getPortMapping();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

  }
}
