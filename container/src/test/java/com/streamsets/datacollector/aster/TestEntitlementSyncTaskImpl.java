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
package com.streamsets.datacollector.aster;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.aster.AsterConfiguration;
import com.streamsets.lib.security.http.aster.AsterRestClient;
import com.streamsets.lib.security.http.aster.AsterService;
import com.streamsets.lib.security.http.aster.AsterServiceProvider;
import org.apache.http.HttpStatus;
import org.eclipse.jetty.security.Authenticator;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import org.mockito.Mockito;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestEntitlementSyncTaskImpl {
  private static final String PRODUCT_ID = "myTestSdcId";
  private static final String BASE_URL = "http://localhost:18630";
  private static final String VERSION = "1.2.3-testVersion";
  private static final String ASTER_URL = "http://test-aster-url";

  private EntitlementSyncTaskImpl task;
  private RuntimeInfo runtimeInfo;
  private BuildInfo buildInfo;
  private Configuration appConfig;
  private Activation activation;
  private AsterService asterService;
  private AsterRestClient asterRestClient;
  private AsterRestClient.Response response;
  private AsterConfiguration asterConfiguration;
  private int responseStatus;

  @Before
  public void setup() {
    runtimeInfo = mock(RuntimeInfo.class);
    when(runtimeInfo.getProductName()).thenReturn(RuntimeInfo.SDC_PRODUCT);
    when(runtimeInfo.getId()).thenReturn(PRODUCT_ID);
    when(runtimeInfo.getBaseHttpUrl()).thenReturn(BASE_URL);

    buildInfo = mock(BuildInfo.class);
    when(buildInfo.getVersion()).thenReturn(VERSION);

    activation = mock(Activation.class);
    // inactive by default, test can override

    appConfig = new Configuration();
    appConfig.set(AsterServiceProvider.ASTER_URL, ASTER_URL);

    asterService = mock(AsterService.class);

    asterRestClient = mock(AsterRestClient.class);
    when(asterService.getRestClient()).thenReturn(asterRestClient);

    asterConfiguration = mock(AsterConfiguration.class);
    when(asterConfiguration.getBaseUrl()).thenReturn(ASTER_URL);
    when(asterService.getConfig()).thenReturn(asterConfiguration);
    AsterContext asterContext = Mockito.mock(AsterContext.class);
    Mockito.when(asterContext.isEnabled()).thenReturn(true);
    Mockito.when(asterContext.getService()).thenReturn(asterService);
    task = spy(new EntitlementSyncTaskImpl(activation, runtimeInfo, buildInfo, appConfig, asterContext));

    doReturn(asterService).when(task).getAsterService();

    response = mock(AsterRestClient.Response.class);
    // fails by default, test can set to successful
    responseStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    when(response.getStatusCode()).thenAnswer(invocation -> responseStatus);

    // avoid real interactions with the world
    doAnswer(invocation -> response)
        .when(task)
        .postToGetEntitlementUrl(anyString(), any(Map.class));
  }

  private void enableAndConfigureActivation(boolean valid) {
    when(activation.isEnabled()).thenReturn(true);
    Activation.Info info = mock(Activation.Info.class);
    when(info.isValid()).thenReturn(valid);
    when(activation.getInfo()).thenReturn(info);
  }

  private void configureSuccessfulResponse(String activationCode) {
    responseStatus = HttpStatus.SC_CREATED;
    Object responseBody = ImmutableMap.of(
        EntitlementSyncTaskImpl.DATA, ImmutableMap.of(
            EntitlementSyncTaskImpl.ACTIVATION_CODE, activationCode
        )
    );
    when(response.getBody()).thenReturn(responseBody);
  }

  @Test
  public void testSuccessfulUpdate() {
    enableAndConfigureActivation(false);
    String activationCode = "testActivationCode";
    configureSuccessfulResponse(activationCode);
    when(asterRestClient.hasTokens()).thenReturn(true);
    task.syncEntitlement();
    verify(task).postToGetEntitlementUrl(
        eq(ASTER_URL + EntitlementSyncTaskImpl.ACTIVATION_ENDPOINT_PATH),
        eq(ImmutableMap.of(
          EntitlementSyncTaskImpl.DATA, ImmutableMap.of(
              "productId", PRODUCT_ID,
              "productType", "DATA_COLLECTOR",
              "productUrl", BASE_URL,
              "productVersion", VERSION
          ),
          "version", 2
    )));
    verify(activation).setActivationKey(activationCode);
  }

  @Test
  public void testSkipSlaveMode() {
    when(runtimeInfo.isClusterSlave()).thenReturn(true);
    task.syncEntitlement();
    verifyNoMoreInteractions(activation);
    verify(task, never()).getAsterService();
    verify(task, never()).postToGetEntitlementUrl(anyString(), any());
  }

  @Test
  public void testSkipInactive() {
    task.syncEntitlement();
    verify(activation).isEnabled();
    verifyNoMoreInteractions(activation);
    verify(task, never()).getAsterService();
    verify(task, never()).postToGetEntitlementUrl(anyString(), any());
  }

  @Test
  public void testSkipAlreadyActivated() {
    enableAndConfigureActivation(true);
    task.syncEntitlement();
    verify(task, never()).getAsterService();
    verify(task, never()).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }

  @Test
  public void testSkipNoCreds() {
    enableAndConfigureActivation(false);
    task.syncEntitlement();
    verify(task).getAsterService();
    verify(asterService).getRestClient();
    verify(asterRestClient).hasTokens();
    verify(task, never()).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }

  @Test
  public void testSkipNoEntitlementUrl() {
    enableAndConfigureActivation(false);
    appConfig.set(AsterServiceProvider.ASTER_URL, "");
    when(task.getAsterService()).thenReturn(null);
    task.syncEntitlement();
    Activation.Info aInfo = activation.getInfo();
    verify(aInfo).isValid();
    verify(task, never()).getAsterService();
    verify(asterRestClient, never()).hasTokens();
    verify(task, never()).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }

  @Test
  public void testResponseException() {
    enableAndConfigureActivation(false);
    when(asterRestClient.hasTokens()).thenReturn(true);
    when(task.postToGetEntitlementUrl(anyString(), any()))
        .thenThrow(new RuntimeException("test response exception"));
    task.syncEntitlement();
    verify(task).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }

  @Test
  public void testUnsuccessfulResponse() {
    enableAndConfigureActivation(false);
    when(asterRestClient.hasTokens()).thenReturn(true);
    task.syncEntitlement();
    verify(task).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }

  @Test
  public void testResponseWithoutActivation() {
    enableAndConfigureActivation(false);
    responseStatus = HttpStatus.SC_OK;
    when(asterRestClient.hasTokens()).thenReturn(true);
    when(response.getBody()).thenReturn(ImmutableMap.of(
        EntitlementSyncTaskImpl.DATA, ImmutableMap.of(
            "wrongKey", "ignored"
        )
    ));
    task.syncEntitlement();
    verify(task).postToGetEntitlementUrl(anyString(), any());
    verify(activation, never()).setActivationKey(any());
  }
}
