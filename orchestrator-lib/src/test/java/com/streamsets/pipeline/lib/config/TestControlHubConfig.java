/*
 * Copyright 2021  StreamSets Inc.
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
package com.streamsets.pipeline.lib.config;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.startJob.StartJobErrors;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestControlHubConfig {

  @Test
  public void testClientBuilderApiUserCredentials() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    com.streamsets.pipeline.api.Configuration conf = Mockito.mock(com.streamsets.pipeline.api.Configuration.class);
    Mockito.when(conf.get(Mockito.eq(ControlHubConfig.ALLOW_API_USER_CREDENTIALS), Mockito.eq(false))).thenReturn(true);
    Mockito.when(context.getConfiguration()).thenReturn(conf);
    ConfigIssue issue = Mockito.mock(ConfigIssue.class);
    Mockito.when(context.createConfigIssue(
        Mockito.eq(com.streamsets.pipeline.lib.startJob.Groups.JOB.name()),
        Mockito.anyString(),
        Mockito.eq(StartJobErrors.START_JOB_09)
    )).thenReturn(issue);
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    ControlHubConfig config = new ControlHubConfig();
    config.baseUrl = "http://foo";
    config.componentId = () -> "componentId";
    config.authToken = () -> "authToken";
    config.authenticationType = AuthenticationType.API_USER_CREDENTIALS;
    config.init(context, issues);
    Assert.assertTrue(issues.isEmpty());

    Configuration clientConf = config.getClientBuilder().getConfiguration();
    Set<Object> instances = clientConf.getInstances();
    Map<Class, Object> instancesMap = instances.stream().collect(Collectors.toMap(e -> e.getClass(), e -> e));
    Assert.assertEquals(ImmutableSet.of(SchCsrfProtectionFilter.class, MovedDpmJerseyClientFilter.class), instancesMap.keySet());
    MovedDpmJerseyClientFilter filter = (MovedDpmJerseyClientFilter) instancesMap.get(MovedDpmJerseyClientFilter.class);
    Assert.assertEquals("http://foo", filter.getClientInfo().getDpmBaseUrl());
    Assert.assertEquals(ImmutableMap.of(
          ControlHubConfig.X_APP_COMPONENT_ID.toLowerCase(),
          "componentId",
          ControlHubConfig.X_APP_AUTH_TOKEN.toLowerCase(),
          "authToken"
        ),
        filter.getClientInfo().getHeaders()
    );

    // API User Credentials disabled
    Mockito.when(conf.get(Mockito.eq(ControlHubConfig.ALLOW_API_USER_CREDENTIALS), Mockito.eq(false))).thenReturn(false);
    config.init(context, issues);
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(issue, issues.get(0));
  }


  @Test
  public void testClientBuilderUserPassword() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    ControlHubConfig config = new ControlHubConfig();
    config.authenticationType = AuthenticationType.USER_PASSWORD;
    config.init(context, issues);

    Configuration clientConf = config.getClientBuilder().getConfiguration();
    Set<Object> instances = clientConf.getInstances();
    Map<Class, Object> instancesMap = instances.stream().collect(Collectors.toMap(e -> e.getClass(), e -> e));
    Assert.assertEquals(ImmutableSet.of(SchCsrfProtectionFilter.class, UserAuthInjectionFilter.class), instancesMap.keySet());
  }

}
