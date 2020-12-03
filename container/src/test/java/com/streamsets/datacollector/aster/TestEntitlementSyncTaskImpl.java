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
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.aster.AsterConfiguration;
import com.streamsets.lib.security.http.aster.AsterRestClient;
import com.streamsets.lib.security.http.aster.AsterService;
import com.streamsets.lib.security.http.aster.AsterServiceProvider;
import org.apache.http.HttpStatus;
import org.eclipse.jetty.security.Authenticator;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestEntitlementSyncTaskImpl {
  private static final String PRODUCT_ID = "myTestSdcId";
  private static final String BASE_URL = "http://localhost:18630";
  private static final String VERSION = "1.2.3-testVersion";
  private static final String ASTER_URL = "http://test-aster-url";
  private static final long RETRY_INITIAL_BACKOFF = 1L;

  private EntitlementSyncTaskImpl task;
  private RuntimeInfo runtimeInfo;
  private BuildInfo buildInfo;
  private Configuration appConfig;
  private Activation activation;
  private AsterService asterService;
  private AsterRestClient asterRestClient;
  private AsterRestClient.Response response;
  private AsterConfiguration asterConfiguration;
  private StatsCollector statsCollector;
  private int responseStatus;

  @Before
  public void setup() {
    runtimeInfo = mock(RuntimeInfo.class);
    when(runtimeInfo.getProductName()).thenReturn(RuntimeInfo.SDC_PRODUCT);
    when(runtimeInfo.getId()).thenReturn(PRODUCT_ID);
    when(runtimeInfo.getBaseHttpUrl(true)).thenReturn(BASE_URL);

    buildInfo = mock(BuildInfo.class);
    when(buildInfo.getVersion()).thenReturn(VERSION);

    activation = mock(Activation.class);
    // inactive by default, test can override

    appConfig = new Configuration();
    appConfig.set(AsterServiceProvider.ASTER_URL, ASTER_URL);

    // make test faster while still testing backoff logic
    appConfig.set(EntitlementSyncTaskImpl.SYNC_RETRY_INITIAL_BACKOFF_CONFIG, RETRY_INITIAL_BACKOFF);

    asterService = mock(AsterService.class);

    asterRestClient = mock(AsterRestClient.class);
    when(asterService.getRestClient()).thenReturn(asterRestClient);

    asterConfiguration = mock(AsterConfiguration.class);
    when(asterConfiguration.getBaseUrl()).thenReturn(ASTER_URL);
    when(asterService.getConfig()).thenReturn(asterConfiguration);
    AsterContext asterContext = Mockito.mock(AsterContext.class);
    Mockito.when(asterContext.isEnabled()).thenReturn(true);
    Mockito.when(asterContext.getService()).thenReturn(asterService);
    statsCollector = mock(StatsCollector.class);
    task = spy(new EntitlementSyncTaskImpl(activation, runtimeInfo, buildInfo, appConfig, asterContext, statsCollector));

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
    verify(statsCollector).setActive(true);
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
  public void testSkipNoEntitlementWhenAsterDisabled() {
    enableAndConfigureActivation(false);
    appConfig.set(AsterServiceProvider.ASTER_URL, "");
    when(task.getAsterService()).thenReturn(null);
    Whitebox.setInternalState(task, "asterContext", new AsterContext() {
      @Override
      public boolean isEnabled() {
        return AsterServiceProvider.isEnabled(appConfig);
      }

      @Override
      public AsterService getService() {
        return null;
      }

      @Override
      public void handleRegistration(ServletRequest req, ServletResponse res) {

      }

      @Override
      public Authenticator getAuthenticator() {
        return null;
      }
    });
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

  @Test
  public void testRetry() {
    // assumptions this test uses
    assertEquals(TimeUnit.SECONDS.toMillis(30), EntitlementSyncTaskImpl.SYNC_TIMEOUT_DEFAULT);
    assertEquals(TimeUnit.MINUTES.toMillis(3), EntitlementSyncTaskImpl.SYNC_MAX_RETRY_WINDOW_DEFAULT);

    when(asterRestClient.hasTokens()).thenReturn(true);
    doCallRealMethod().when(task).postToGetEntitlementUrl(anyString(), any());
    long startTime = System.currentTimeMillis();
    when(task.getTime()).thenReturn(
        startTime, // initialization
        startTime + TimeUnit.SECONDS.toMillis(31), // after first call fails, slightly longer than timeout
        startTime + TimeUnit.SECONDS.toMillis(62),
        startTime + TimeUnit.SECONDS.toMillis(93),
        startTime + TimeUnit.SECONDS.toMillis(124),
        startTime + TimeUnit.SECONDS.toMillis(155),
        startTime + TimeUnit.SECONDS.toMillis(186) // outside of retry window
    );
    responseStatus = 200;
    AtomicInteger throwCount = new AtomicInteger(3); // fail three times, then succeed
    doAnswer(args -> {
      int currentCount = throwCount.getAndDecrement();
      if (currentCount > 0) {
        switch (currentCount) {
          case 2:
            throw new HttpServerErrorException("500 Internal Server Error");
          case 3:
            throw new RuntimeException(new HttpServerErrorException("502 Bad Gateway"));
          case 4:
            throw new RuntimeException("wrapper message is ignored",
                new HttpServerErrorException("503 Service Unavailable"));
          case 5:
            throw new HttpServerErrorException("504 Gateway Timeout");
          default: // also case 1
            throw new RuntimeException("test wrapper", new SocketTimeoutException("test ex"));
        }
      }
      return response;
    }).when(asterRestClient).doRestCall(any());

    assertSame(response, task.postToGetEntitlementUrl("unused", ImmutableMap.of("not", "used")));
    verify(asterRestClient, times(4)).doRestCall(any());
    verify(task).sleep(1 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(2 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(4 * RETRY_INITIAL_BACKOFF);
    verify(task, never()).sleep(eq(8 * RETRY_INITIAL_BACKOFF));
    verify(task, never()).sleep(eq(16 * RETRY_INITIAL_BACKOFF));
    verify(task, never()).sleep(eq(32 * RETRY_INITIAL_BACKOFF));

    // test max retries
    when(task.getTime()).thenReturn(
        startTime, // initialization
        startTime + TimeUnit.SECONDS.toMillis(31), // after first call fails, slightly longer than timeout
        startTime + TimeUnit.SECONDS.toMillis(62),
        startTime + TimeUnit.SECONDS.toMillis(93),
        startTime + TimeUnit.SECONDS.toMillis(124),
        startTime + TimeUnit.SECONDS.toMillis(155),
        startTime + TimeUnit.SECONDS.toMillis(186) // outside of retry window, 7th try is not attempted
    );
    throwCount.set(6);
    try {
      task.postToGetEntitlementUrl("unused", ImmutableMap.of("not", "used"));
      fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("test wrapper", e.getMessage());
    }
    verify(asterRestClient, times(10)).doRestCall(any());
    // expect 5 more sleeps
    verify(task, times(2)).sleep(1 * RETRY_INITIAL_BACKOFF);
    verify(task, times(2)).sleep(2 * RETRY_INITIAL_BACKOFF);
    verify(task, times(2)).sleep(4 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(8 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(16 * RETRY_INITIAL_BACKOFF);
    verify(task, never()).sleep(eq(32 * RETRY_INITIAL_BACKOFF));

    // test retries of various status codes
    when(task.getTime()).thenReturn(
        startTime, // initialization
        startTime + TimeUnit.SECONDS.toMillis(31), // after first call fails, slightly longer than timeout
        startTime + TimeUnit.SECONDS.toMillis(62),
        startTime + TimeUnit.SECONDS.toMillis(93),
        startTime + TimeUnit.SECONDS.toMillis(124),
        startTime + TimeUnit.SECONDS.toMillis(155),
        startTime + TimeUnit.SECONDS.toMillis(186) // outside of retry window
    );
    when(response.getStatusCode()).thenReturn(504, 503, 502, 500, 200);
    assertSame(response, task.postToGetEntitlementUrl("unused", ImmutableMap.of("not", "used")));
    verify(asterRestClient, times(15)).doRestCall(any());
    // expect 4 more sleeps
    verify(task, times(3)).sleep(1 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(2 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(4 * RETRY_INITIAL_BACKOFF);
    verify(task, times(2)).sleep(8 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(16 * RETRY_INITIAL_BACKOFF);
    verify(task, never()).sleep(eq(32 * RETRY_INITIAL_BACKOFF));

    // test interrupted
    when(task.getTime()).thenReturn(
        startTime, // initialization
        startTime + TimeUnit.SECONDS.toMillis(31), // after first call fails, slightly longer than timeout
        startTime + TimeUnit.SECONDS.toMillis(62),
        startTime + TimeUnit.SECONDS.toMillis(93),
        startTime + TimeUnit.SECONDS.toMillis(124),
        startTime + TimeUnit.SECONDS.toMillis(155),
        startTime + TimeUnit.SECONDS.toMillis(186) // outside of retry window
    );
    // make it fail
    throwCount.set(1);
    // interrupted
    when(task.sleep(anyLong())).thenReturn(false);
    try {
      task.postToGetEntitlementUrl("unused", ImmutableMap.of("not", "used"));
      fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("test wrapper", e.getMessage());
    }
    // 1 more rest call, retry was aborted
    verify(asterRestClient, times(16)).doRestCall(any());
    // 1 more sleep
    verify(task, times(4)).sleep(1 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(2 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(4 * RETRY_INITIAL_BACKOFF);
    verify(task, times(2)).sleep(8 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(16 * RETRY_INITIAL_BACKOFF);
    verify(task, never()).sleep(eq(32 * RETRY_INITIAL_BACKOFF));

    // test non-retriable exception
    when(task.getTime()).thenReturn(
        startTime, // initialization
        startTime + TimeUnit.SECONDS.toMillis(31), // after first call fails, slightly longer than timeout
        startTime + TimeUnit.SECONDS.toMillis(62),
        startTime + TimeUnit.SECONDS.toMillis(93),
        startTime + TimeUnit.SECONDS.toMillis(124),
        startTime + TimeUnit.SECONDS.toMillis(155),
        startTime + TimeUnit.SECONDS.toMillis(186) // outside of retry window, 7th try is not attempted
    );
    doThrow(new RuntimeException("other exception")).when(asterRestClient).doRestCall(any());
    try {
      task.postToGetEntitlementUrl("unused", ImmutableMap.of("not", "used"));
      fail("Expected exception");
    } catch (RuntimeException e) {
      assertEquals("other exception", e.getMessage());
    }
    verify(asterRestClient, times(17)).doRestCall(any());
    // expect 0 more sleeps, no retries
    verify(task, times(4)).sleep(1 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(2 * RETRY_INITIAL_BACKOFF);
    verify(task, times(3)).sleep(4 * RETRY_INITIAL_BACKOFF);
    verify(task, times(2)).sleep(8 * RETRY_INITIAL_BACKOFF);
    verify(task).sleep(16 * RETRY_INITIAL_BACKOFF);
    verify(task, never()).sleep(eq(32 * RETRY_INITIAL_BACKOFF));
  }
}
