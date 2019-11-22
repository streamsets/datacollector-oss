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

public class TestActiveActivePortsForwarding {

  @Test
  public void testTunnelsLifecycle() {
    SshTunnel tunnel = Mockito.mock(SshTunnel.class);
    Mockito.when(tunnel.getTunnelEntryHost()).thenReturn("L");
    Mockito.when(tunnel.getTunnelEntryPort()).thenReturn(2);

    SshTunnel.Builder tunnelBuilder = Mockito.mock(SshTunnel.Builder.class);
    Mockito.when(tunnelBuilder.build()).thenReturn(tunnel);

    SshTunnelService.HostPort hostPort = new SshTunnelService.HostPort("l", 1);

    ActivePortsForwarding portsForwarding = new ActivePortsForwarding(tunnelBuilder, ImmutableList.of(hostPort));

    Assert.assertTrue(portsForwarding.isEnabled());

    portsForwarding.start();

    Mockito.verify(tunnel, Mockito.times(1)).start(Mockito.eq("l"), Mockito.eq(1));

    Assert.assertEquals(ImmutableList.of(tunnel), portsForwarding.getSshTunnels());

    Assert.assertEquals(
        ImmutableMap.of(hostPort, new SshTunnelService.HostPort("L", 2)),
        portsForwarding.getPortMapping()
    );

    Mockito.verify(tunnel, Mockito.times(0)).healthCheck();
    portsForwarding.healthCheck();
    Mockito.verify(tunnel, Mockito.times(1)).healthCheck();

    portsForwarding.stop();
  }

  @Test
  public void testTunnelsIllegalStates() {
    SshTunnel tunnel = Mockito.mock(SshTunnel.class);
    Mockito.when(tunnel.getTunnelEntryHost()).thenReturn("L");
    Mockito.when(tunnel.getTunnelEntryPort()).thenReturn(2);

    SshTunnel.Builder tunnelBuilder = Mockito.mock(SshTunnel.Builder.class);
    Mockito.when(tunnelBuilder.build()).thenReturn(tunnel);

    SshTunnelService.HostPort hostPort = new SshTunnelService.HostPort("l", 1);

    ActivePortsForwarding portsForwarding = new ActivePortsForwarding(tunnelBuilder, ImmutableList.of(hostPort));

    try {
      portsForwarding.stop();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    try {
      portsForwarding.getSshTunnels();
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

    portsForwarding.getSshTunnels();
    portsForwarding.getPortMapping();
    tunnel.stop();
    portsForwarding.stop();

    try {
      portsForwarding.start();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    try {
      portsForwarding.getSshTunnels();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

    try {
      portsForwarding.getPortMapping();
      Assert.fail();
    } catch (IllegalStateException ex) {
    }

  }
}
