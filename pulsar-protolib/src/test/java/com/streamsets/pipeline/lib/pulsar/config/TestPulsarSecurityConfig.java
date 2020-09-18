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
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

import java.io.File;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AuthenticationFactory.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestPulsarSecurityConfig {

  private PulsarSecurityConfig pulsarSecurityConfig;
  private static File caCertFile;
  private static File clientCertFile;
  private static File clientKeyFile;
  private static String resourceDirectoryPath;

  // Mocks
  private Stage.Context contextMock;
  private ClientBuilder clientBuilderMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void before() {
    caCertFile = new File(TestPulsarSecurityConfig.class.getClassLoader().getResource("cacert.pem").getPath());
    clientCertFile = new File(TestPulsarSecurityConfig.class.getClassLoader().getResource("client-cert.pem").getPath());
    clientKeyFile = new File(TestPulsarSecurityConfig.class.getClassLoader().getResource("client-key.pem").getPath());
    resourceDirectoryPath = caCertFile.getParent();
  }

  @Before
  public void setUp() {
    pulsarSecurityConfig = new PulsarSecurityConfig();
    pulsarSecurityConfig.tlsEnabled = false;
    pulsarSecurityConfig.caCertPem = null;
    pulsarSecurityConfig.tlsAuthEnabled = false;
    pulsarSecurityConfig.clientCertPem = null;
    pulsarSecurityConfig.clientKeyPem = null;

    contextMock = Mockito.mock(Stage.Context.class);
    Mockito.when(contextMock.getResourcesDirectory()).thenReturn(resourceDirectoryPath);

    clientBuilderMock = Mockito.mock(ClientBuilder.class);
  }

  @Test
  public void initTlsDisabledTlsAuthDisabledSuccess() {
    Assert.assertTrue(pulsarSecurityConfig.init(contextMock).isEmpty());
  }

  @Test
  public void initTlsEnabledTlsAuthDisabledSuccess() {
    pulsarSecurityConfig.caCertPem = caCertFile.getName();

    pulsarSecurityConfig.tlsEnabled = true;
    Assert.assertTrue(pulsarSecurityConfig.init(contextMock).isEmpty());
  }

  @Test
  public void initTlsEnabledTlsAuthEnabledSuccess() {
    pulsarSecurityConfig.caCertPem = caCertFile.getName();
    pulsarSecurityConfig.clientCertPem = clientCertFile.getName();
    pulsarSecurityConfig.clientKeyPem = clientKeyFile.getName();

    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.tlsAuthEnabled = true;
    Assert.assertTrue(pulsarSecurityConfig.init(contextMock).isEmpty());
  }

  @Test
  public void initTlsEnabledTlsAuthDisabledCaCertIssues() {
    // Check PULSAR_11 issue -> cacert file path cannot be null
    Mockito.when(contextMock.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.eq(PulsarErrors.PULSAR_11)))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_11.getCode();
             }
           });

    pulsarSecurityConfig.tlsEnabled = true;
    List<ConfigIssue> issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_11.getCode()));

    // Check PULSAR_12 issue -> cacert file path not found issue
    Mockito.when(contextMock.createConfigIssue(Mockito.any(),
        Mockito.any(),
        Mockito.eq(PulsarErrors.PULSAR_12),
        Mockito.anyString(),
        Mockito.anyString()
    ))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_12.getCode();
             }
           });

    pulsarSecurityConfig.caCertPem = "wrong-file-name.pem";
    issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_12.getCode()));
  }

  @Test
  public void initTlsEnabledTlsAuthEnabledClientCertIssues() {
    // Check PULSAR_13 issue -> client cert file path cannot be null
    Mockito.when(contextMock.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.eq(PulsarErrors.PULSAR_13)))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_13.getCode();
             }
           });

    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.tlsAuthEnabled = true;
    pulsarSecurityConfig.caCertPem = "cacert.pem";
    List<ConfigIssue> issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_13.getCode()));

    // Check PULSAR_14 issue -> client cert file path not found issue
    Mockito.when(contextMock.createConfigIssue(Mockito.any(),
        Mockito.any(),
        Mockito.eq(PulsarErrors.PULSAR_14),
        Mockito.anyString(),
        Mockito.anyString()
    ))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_14.getCode();
             }
           });

    pulsarSecurityConfig.clientCertPem = "wrong-file-name.pem";
    issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_14.getCode()));
  }

  @Test
  public void initTlsEnabledTlsAuthEnabledClientKeyIssues() {
    // Check PULSAR_15 issue -> client key file path cannot be null
    Mockito.when(contextMock.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.eq(PulsarErrors.PULSAR_15)))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_15.getCode();
             }
           });

    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.tlsAuthEnabled = true;
    pulsarSecurityConfig.caCertPem = "cacert.pem";
    pulsarSecurityConfig.clientCertPem = "client-cert.pem";
    List<ConfigIssue> issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_15.getCode()));

    // Check PULSAR_16 issue -> client key file path not found issue
    Mockito.when(contextMock.createConfigIssue(Mockito.any(),
        Mockito.any(),
        Mockito.eq(PulsarErrors.PULSAR_16),
        Mockito.anyString(),
        Mockito.anyString()
    ))
           .thenReturn(new ConfigIssue() {
             @Override
             public String toString() {
               return PulsarErrors.PULSAR_16.getCode();
             }
           });

    pulsarSecurityConfig.clientKeyPem = "wrong-file-name.pem";
    issues = pulsarSecurityConfig.init(contextMock);
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(PulsarErrors.PULSAR_16.getCode()));
  }

  @Test
  public void configurePulsarBuilderTlsDisabledAuthDisabledSuccess() throws StageException {
    pulsarSecurityConfig.configurePulsarBuilder(clientBuilderMock);
  }

  @Test
  public void configurePulsarBuilderTlsEnabledAuthDisabledSuccess() throws StageException {
    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.caCertPem = "cacert.pem";
    pulsarSecurityConfig.init(contextMock);
    pulsarSecurityConfig.configurePulsarBuilder(clientBuilderMock);
  }

  @Test
  public void configurePulsarBuilderTlsEnabledAuthEnabledSuccess() throws StageException {
    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.tlsAuthEnabled = true;
    pulsarSecurityConfig.caCertPem = "cacert.pem";
    pulsarSecurityConfig.clientCertPem = "client-cert.pem";
    pulsarSecurityConfig.clientKeyPem = "client-key.pem";
    pulsarSecurityConfig.init(contextMock);
    pulsarSecurityConfig.configurePulsarBuilder(clientBuilderMock);
  }

  @Test(expected = StageException.class)
  public void configurePulsarBuilderTlsEnabledAuthEnabledStageException()
      throws StageException, PulsarClientException.UnsupportedAuthenticationException {
    pulsarSecurityConfig.tlsEnabled = true;
    pulsarSecurityConfig.tlsAuthEnabled = true;
    pulsarSecurityConfig.caCertPem = "cacert.pem";
    pulsarSecurityConfig.clientCertPem = null;
    pulsarSecurityConfig.clientKeyPem = null;

    PowerMockito.mockStatic(AuthenticationFactory.class);
    BDDMockito.given(AuthenticationFactory.create(Mockito.anyString(), Mockito.anyMap()))
              .willThrow(new PulsarClientException.UnsupportedAuthenticationException("message"));

    pulsarSecurityConfig.init(contextMock);
    pulsarSecurityConfig.configurePulsarBuilder(clientBuilderMock);
  }
}
