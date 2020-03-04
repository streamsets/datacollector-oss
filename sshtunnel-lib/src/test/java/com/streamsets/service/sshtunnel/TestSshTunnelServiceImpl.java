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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.service.sshtunnel.SshTunnelService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;

public class TestSshTunnelServiceImpl {

  @Test
  public void testServiceDisabled() {
    SshTunnelConfigBean config = new SshTunnelConfigBean();
    config.sshTunneling = false;
    SshTunnelServiceImpl service = new SshTunnelServiceImpl();
    service.sshTunnelConfig = config;

    Assert.assertTrue(service.init().isEmpty());
    Assert.assertFalse(service.isEnabled());

    SshTunnelService.HostPort hostPort = new SshTunnelService.HostPort("l", 1);

    Map<SshTunnelService.HostPort, SshTunnelService.HostPort> map = service.start(ImmutableList.of(hostPort));
    Assert.assertNotNull(map);
    Assert.assertEquals(ImmutableMap.of(hostPort, hostPort), map);
    service.destroy();
  }

  @Test
  public void testServiceEnabled() {
    DataCollectorServices.instance().put(JsonMapper.SERVICE_KEY, new JsonMapper() {
      @Override
      public String writeValueAsString(Object value) throws IOException {
        return null;
      }

      @Override
      public byte[] writeValueAsBytes(Object value) throws IOException {
        return new byte[0];
      }

      @Override
      public <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
        return null;
      }

      @Override
      public <T> T readValue(String src, Class<T> valueType) throws IOException {
        return new ObjectMapper().readValue(src, valueType);
      }

      @Override
      public boolean isValidJson(String json) {
        return false;
      }
    });

    SshTunnelConfigBean config = new SshTunnelConfigBean();
    config.sshTunneling = true;
    config.sshHost = "l";
    config.sshPort = 1;
    config.sshUsername = "user";
    config.sshHostFingerprints = "f1,f2,";
    config.sshPrivateKey = () -> "priv";
    config.sshPrivateKeyPassword = () -> "pass";
    config.sshPublicKey = () -> "pub";

    SshTunnelServiceImpl service = new SshTunnelServiceImpl();
    service = Mockito.spy(service);

    SshTunnelService.HostPort target = new SshTunnelService.HostPort("K", 3);
    SshTunnelService.HostPort forwarder = new SshTunnelService.HostPort("L", 4);

    SshTunnel tunnel = Mockito.mock(SshTunnel.class);
    Mockito.when(tunnel.start(Mockito.eq(ImmutableList.of(target)))).thenReturn(ImmutableMap.of(target, forwarder));
    SshTunnel.Builder tunnelBuilder = Mockito.mock(SshTunnel.Builder.class);
    Mockito.when(tunnelBuilder.setSshHost(Mockito.anyString())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshPort(Mockito.anyInt())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshUser(Mockito.anyString())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshHostFingerprints(Mockito.anyList())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshPrivateKey(Mockito.anyString())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshPublicKey(Mockito.anyString())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshPrivateKeyPassword(Mockito.anyString())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setReadyTimeOut(Mockito.anyInt())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.setSshKeepAlive(Mockito.anyInt())).thenReturn(tunnelBuilder);
    Mockito.when(tunnelBuilder.build()).thenReturn(tunnel);
    Mockito.doReturn(tunnelBuilder).when(service).createSshTunnelBuilder();

    service.sshTunnelConfig = config;

    Assert.assertTrue(service.init().isEmpty());
    Assert.assertTrue(service.isEnabled());

    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshHost(Mockito.eq("l"));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshPort(Mockito.eq(1));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshUser(Mockito.eq("user"));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshHostFingerprints(Mockito.eq(ImmutableList.of("f1", "f2")));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshPrivateKey(Mockito.eq("priv"));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshPublicKey(Mockito.eq("pub"));
    Mockito.verify(tunnelBuilder, Mockito.times(1)).setSshPrivateKeyPassword(Mockito.eq("pass"));

    Map<SshTunnelService.HostPort, SshTunnelService.HostPort> map = service.start(ImmutableList.of(target));
    Assert.assertNotNull(map);
    Assert.assertEquals(ImmutableMap.of(target, forwarder), map);

    service.healthCheck();

    service.stop();

    try {
      service.healthCheck();
      Assert.fail();
    } catch (IllegalStateException ex) {

    }

    service.destroy();

  }


}
