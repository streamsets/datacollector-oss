/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.lib.security.http;

import com.streamsets.datacollector.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class TestFailoverSSOService {

  @Test
  public void testMethodsDelegation() throws Exception {
    RemoteSSOService remote = Mockito.mock(RemoteSSOService.class);
    DisconnectedSSOService disconnected = Mockito.mock(DisconnectedSSOService.class);
    FailoverSSOService failover = new FailoverSSOService(remote, disconnected);
    failover = Mockito.spy(failover);

    Configuration conf = new Configuration();
    failover.setConfiguration(conf);
    Mockito.verify(remote, Mockito.times(1)).setConfiguration(Mockito.eq(conf));
    Mockito.verify(disconnected, Mockito.times(1)).setConfiguration(Mockito.eq(conf));

    Mockito.doNothing().when(failover).failoverIfRemoteNotActive(false);
    Mockito.verify(failover, Mockito.never()).failoverIfRemoteNotActive(Mockito.eq(false));

    // register
    Map<String, String> attribures = Collections.emptyMap();
    failover.register(attribures);
    Mockito.verify(remote, Mockito.times(1)).register(Mockito.eq(attribures));
    Mockito.verify(disconnected, Mockito.times(1)).setConfiguration(Mockito.eq(conf));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(false));

    // test if active & delegation to active
    SSOService active = Mockito.mock(SSOService.class);
    Mockito.doReturn(active).when(failover).getActiveService();

    failover.createRedirectToLoginUrl("url", true, null, null);
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).createRedirectToLoginUrl(Mockito.eq("url"), Mockito.eq(true),
        Mockito.any(),
        Mockito.any()
    );

    failover.getLogoutUrl();
    Mockito.verify(failover, Mockito.times(2)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).getLogoutUrl();

    failover.validateUserToken("t");
    Mockito.verify(failover, Mockito.times(3)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).validateUserToken(Mockito.eq("t"));

    failover.invalidateUserToken("t");
    Mockito.verify(failover, Mockito.times(4)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).invalidateUserToken(Mockito.eq("t"));

    failover.validateAppToken("a", "c");
    Mockito.verify(failover, Mockito.times(5)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).validateAppToken(Mockito.eq("a"), Mockito.eq("c"));

    failover.invalidateAppToken("a");
    Mockito.verify(failover, Mockito.times(6)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).invalidateAppToken(Mockito.eq("a"));

    failover.clearCaches();
    Mockito.verify(failover, Mockito.times(7)).failoverIfRemoteNotActive(Mockito.eq(false));
    Mockito.verify(failover, Mockito.times(1)).failoverIfRemoteNotActive(Mockito.eq(true));
    Mockito.verify(failover, Mockito.times(1)).clearCaches();
  }

  @Test
  public void testFailover() throws Exception {
    RemoteSSOService remote = Mockito.mock(RemoteSSOService.class);
    DisconnectedSSOService disconnected = Mockito.mock(DisconnectedSSOService.class);
    FailoverSSOService failover = new FailoverSSOService(remote, disconnected);
    failover = Mockito.spy(failover);

    Mockito.doReturn(null).when(failover).runRecovery();

    Configuration conf = new Configuration();
    failover.setConfiguration(conf);
    Map<String, String> attribures = Collections.emptyMap();

    Mockito.reset(disconnected);
    // registration

    // remote active
    Mockito.reset(remote);
    Mockito.when(remote.isServiceActive(false)).thenReturn(true);

    failover.register(attribures);

    Mockito.verify(remote, Mockito.times(1)).isServiceActive(false);
    Mockito.verifyZeroInteractions(disconnected);
    Assert.assertEquals(remote, failover.getActiveService());

    // remote not active
    Mockito.reset(remote);
    Mockito.when(remote.isServiceActive(false)).thenReturn(false);

    Assert.assertFalse(failover.isRecoveryInProgress());

    failover.register(attribures);

    Mockito.verify(remote, Mockito.times(1)).isServiceActive(false);
    Mockito.verify(disconnected, Mockito.times(1)).register(Mockito.eq(attribures));
    Assert.assertEquals(disconnected, failover.getActiveService());
    Assert.assertTrue(failover.isRecoveryInProgress());
    Mockito.verify(failover, Mockito.times(1)).runRecovery();

    // recovery is already in progress, it should not be triggered again
    failover.failoverIfRemoteNotActive(false);
    Mockito.verify(failover, Mockito.times(1)).runRecovery();

    failover.setRecoveryInProgress(false);

    // recovery not in progress, it should  be triggered again
    failover.failoverIfRemoteNotActive(false);
    Mockito.verify(failover, Mockito.times(2)).runRecovery();

    // failover with immediate check
    remote = Mockito.mock(RemoteSSOService.class);
    disconnected = Mockito.mock(DisconnectedSSOService.class);
    failover = new FailoverSSOService(remote, disconnected);
    failover = Mockito.spy(failover);
    Mockito.reset(remote);
    Mockito.when(remote.isServiceActive(false)).thenReturn(true);
    Mockito.verify(remote, Mockito.never()).isServiceActive(Mockito.eq(true));
    failover.failoverIfRemoteNotActive(true);
    Mockito.verify(remote, Mockito.times(1)).isServiceActive(Mockito.eq(true));
  }

  @Test
  public void testRunRecoveryRemoteRecovers() throws Exception {
    RemoteSSOService remote = Mockito.mock(RemoteSSOService.class);
    Mockito.when(remote.isServiceActive(false)).thenReturn(false);
    DisconnectedSSOService disconnected = Mockito.mock(DisconnectedSSOService.class);
    FailoverSSOService failover = new FailoverSSOService(remote, disconnected);
    failover = Mockito.spy(failover);
    Configuration conf = new Configuration();
    failover.setConfiguration(conf);
    Map<String, String> attribures = Collections.emptyMap();

    //goes to disconnected
    failover.register(attribures);
    Assert.assertEquals(disconnected, failover.getActiveService());

    // remote recovers
    Mockito.verify(failover, Mockito.times(1)).setRecoveryInProgress(Mockito.eq(true));
    Mockito.reset(remote);
    Mockito.when(remote.isServiceActive(false)).thenReturn(true);

    Thread thread = failover.runRecovery();
    thread.join();
    Assert.assertEquals(remote, failover.getActiveService());
  }

  @Test
  public void testRunRecoveryRemoteDoesNotRecover() throws Exception {
    RemoteSSOService remote = Mockito.mock(RemoteSSOService.class);
    Mockito.when(remote.isServiceActive(false)).thenReturn(false);
    DisconnectedSSOService disconnected = Mockito.mock(DisconnectedSSOService.class);
    FailoverSSOService failover = new FailoverSSOService(remote, disconnected);
    failover = Mockito.spy(failover);
    Configuration conf = new Configuration();
    failover.setConfiguration(conf);
    Map<String, String> attribures = Collections.emptyMap();

    //goes to disconnected
    failover.register(attribures);
    Assert.assertEquals(disconnected, failover.getActiveService());

    // remote does not recover
    Mockito.verify(failover, Mockito.times(1)).setRecoveryInProgress(Mockito.eq(true));
    Mockito.reset(remote);
    Mockito.when(remote.isServiceActive(false)).thenReturn(false);

    Thread thread = failover.runRecovery();
    thread.join();
    Assert.assertEquals(disconnected, failover.getActiveService());
  }

}
