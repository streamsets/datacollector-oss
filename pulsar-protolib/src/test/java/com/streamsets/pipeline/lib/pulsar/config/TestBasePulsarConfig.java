/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.pulsar.config;

import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.Utils.TestUtilsPulsar;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PulsarClient.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestBasePulsarConfig {

  private BasePulsarConfig basePulsarConfig;

  // Mocks
  private static Stage.Context contextMock;
  private static PulsarSecurityConfig pulsarSecurityConfigMock;
  private static ClientBuilder clientBuilderMock;
  private static PulsarClient pulsarClientMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws PulsarClientException {
    // New mocks are needed when using Mockito.times() as each mock counts the number of times a method has been called
    contextMock = Mockito.mock(Stage.Context.class);
    pulsarSecurityConfigMock = Mockito.mock(PulsarSecurityConfig.class);
    pulsarClientMock = Mockito.mock(PulsarClient.class);
    clientBuilderMock = Mockito.mock(ClientBuilder.class);

    PowerMockito.mockStatic(PulsarClient.class);
    BDDMockito.given(PulsarClient.builder()).willReturn(clientBuilderMock);

    Mockito.when(pulsarSecurityConfigMock.init(contextMock)).thenReturn(Collections.emptyList());

    Mockito.when(clientBuilderMock.serviceUrl(Mockito.anyString())).thenReturn(clientBuilderMock);
    Mockito.when(clientBuilderMock.keepAliveInterval(Mockito.anyInt(), Mockito.any())).thenReturn(clientBuilderMock);
    Mockito.when(clientBuilderMock.operationTimeout(Mockito.anyInt(), Mockito.any())).thenReturn(clientBuilderMock);
    Mockito.when(clientBuilderMock.build()).thenReturn(pulsarClientMock);

    basePulsarConfig = new BasePulsarConfig();
    basePulsarConfig.serviceURL = "http://localhost:8080";
    basePulsarConfig.keepAliveInterval = 30000;
    basePulsarConfig.operationTimeout = 30000;
    basePulsarConfig.properties = new HashMap<>();
    basePulsarConfig.securityConfig = pulsarSecurityConfigMock;
  }

  @Test
  public void initSuccess() {
    Assert.assertTrue(basePulsarConfig.init(contextMock).isEmpty());
  }

  @Test
  public void initSecurityConfigIssues() {
    Mockito.when(pulsarSecurityConfigMock.init(contextMock)).thenReturn(TestUtilsPulsar.getConfigIssues());
    Assert.assertEquals(1, basePulsarConfig.init(contextMock).size());
  }

  @Test
  public void initClientIssues() throws PulsarClientException, StageException {
    Mockito.when(clientBuilderMock.build()).thenThrow(new PulsarClientException("pulsar client exception in build"));
    Mockito.when(contextMock.createConfigIssue(Mockito.any(),
        Mockito.anyString(),
        Mockito.eq(PulsarErrors.PULSAR_00),
        Mockito.any(),
        Mockito.any()
    )).thenReturn(new ConfigIssue() {
      @Override
      public String toString() {
        return PulsarErrors.PULSAR_00.getCode();
      }
    });

    Assert.assertTrue(basePulsarConfig.init(contextMock).get(0).toString().contains(PulsarErrors.PULSAR_00.getCode()));
    Mockito.verify(pulsarSecurityConfigMock, Mockito.times(1)).configurePulsarBuilder(clientBuilderMock);
    Mockito.verify(clientBuilderMock, Mockito.times(1)).build();
  }

  @Test
  public void destroyClientNotNullSuccess() throws PulsarClientException {
    Assert.assertTrue(basePulsarConfig.init(contextMock).isEmpty());
    basePulsarConfig.destroy();
    Mockito.verify(pulsarClientMock, Mockito.times(1)).close();
  }

  @Test
  public void destroyClientNullSuccess() throws PulsarClientException {
    basePulsarConfig.destroy();
    Mockito.verify(pulsarClientMock, Mockito.never()).close();
  }

  @Test
  public void extraInitSuccess() {
    Assert.assertTrue(basePulsarConfig.extraInit(contextMock).isEmpty());
  }

  @Test
  public void extraBuilderConfigurationSuccess() {
    Assert.assertTrue(basePulsarConfig.extraBuilderConfiguration(clientBuilderMock).isEmpty());
  }

}
