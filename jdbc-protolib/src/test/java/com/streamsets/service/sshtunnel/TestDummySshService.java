/*
 * Copyright 2020 StreamSets Inc.
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

import java.util.Map;

public class TestDummySshService {

  @Test
  public void testServiceDisabled() {
    SshTunnelService service = new DummySshService();

    Assert.assertTrue(service.isEnabled());
  }

  @Test
  public void testServiceEnabled() {
    SshTunnelService service = new DummySshService();
    service = Mockito.spy(service);

    SshTunnelService.HostPort target = new SshTunnelService.HostPort("K", 3);

    Assert.assertTrue(service.isEnabled());

    Map<SshTunnelService.HostPort, SshTunnelService.HostPort> map = service.start(ImmutableList.of(target));
    Assert.assertNotNull(map);
    Assert.assertEquals(ImmutableMap.of(target, target), map);

    service.healthCheck();

    service.stop();

  }
}
