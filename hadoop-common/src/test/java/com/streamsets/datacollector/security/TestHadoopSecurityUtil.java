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
package com.streamsets.datacollector.security;

import com.streamsets.pipeline.api.Stage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static com.streamsets.testing.Matchers.fieldWithValue;

public class TestHadoopSecurityUtil {

  @Test
  public void testGetLoginUser() throws Exception {
    final Configuration conf = new Configuration();
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation ugi = HadoopSecurityUtil.getLoginUser(conf);
    Assert.assertEquals(loginUser.getUserName(), ugi.getUserName());
  }

  @Test
  public void testGetProxyUser() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("foo", new String[] { "all" });
    Stage.Context context = mock(Stage.Context.class);
    com.streamsets.pipeline.api.Configuration configuration = mock(com.streamsets.pipeline.api.Configuration.class);
    when(context.getConfiguration()).thenReturn(configuration);
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    UserGroupInformation ugi = HadoopSecurityUtil.getProxyUser(
      "proxy",
      context,
      fooUgi,
      issues,
      "config",
      "userName"
    );
    Assert.assertEquals("proxy", ugi.getUserName());
  }

  Stage.UserContext userContext = new Stage.UserContext() {
    public String getUser() {
      return "FOO@bar";
    }
    public String getAliasName() {
      return "FOO";
    }
  };

  @Test
  public void testGetProxyUserEnforceCurrentUser() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("foo", new String[] { "all" });
    Stage.Context context = mock(Stage.Context.class);
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    com.streamsets.pipeline.api.Configuration configuration = mock(com.streamsets.pipeline.api.Configuration.class);
    when(configuration.get(eq(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER), eq(false))).thenReturn(true);
    when(context.getConfiguration()).thenReturn(configuration);
    when(context.getUserContext()).thenReturn(userContext);

    UserGroupInformation ugi = HadoopSecurityUtil.getProxyUser(
      "",
      context,
      fooUgi,
      issues,
      "config",
      "userName"
    );
    Assert.assertEquals("FOO", ugi.getUserName());
  }

  @Test
  public void testGetProxyUserCantSpecifyUserWhenEnforcingCurrentUser() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("foo", new String[] { "all" });
    Stage.Context context = mock(Stage.Context.class);

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    com.streamsets.pipeline.api.Configuration configuration = mock(com.streamsets.pipeline.api.Configuration.class);
    when(configuration.get(anyString(), eq(false))).thenReturn(true);
    when(context.getConfiguration()).thenReturn(configuration);
    when(context.getUserContext()).thenReturn(userContext);

    HadoopSecurityUtil.getProxyUser(
      "employee-of-the-year",
      context,
      fooUgi,
      issues,
      "config",
      "userName"
    );

    Assert.assertEquals(1, issues.size());
  }

  @Test
  public void testGetProxyLowerCaseUser() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("BRYAN", new String[] { "all" });
    Stage.Context context = mock(Stage.Context.class);
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    com.streamsets.pipeline.api.Configuration configuration = mock(com.streamsets.pipeline.api.Configuration.class);
    when(configuration.get(eq(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER), eq(false))).thenReturn(true);
    when(configuration.get(eq(HadoopConfigConstants.LOWERCASE_USER), eq(false))).thenReturn(true);
    when(context.getConfiguration()).thenReturn(configuration);
    when(context.getUserContext()).thenReturn(userContext);

    UserGroupInformation ugi = HadoopSecurityUtil.getProxyUser(
      "",
      context,
      fooUgi,
      issues,
      "config",
      "userName"
    );
    Assert.assertEquals("foo", ugi.getUserName());
  }

  @Test(expected = IllegalImpersonationException.class)
  public void testIllegalImpersonation() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("foo", new String[] { "all" });

    com.streamsets.datacollector.util.Configuration sdcConf = new com.streamsets.datacollector.util.Configuration();
    sdcConf.set(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER, true);
    HadoopSecurityUtil.getProxyUser(
        "requested",
        "current",
        sdcConf,
        fooUgi
    );
  }

  @Test
  public void testProxyUserNewOverload() throws Exception {
    final UserGroupInformation fooUgi = UserGroupInformation.createUserForTesting("foo", new String[] { "all" });

    com.streamsets.datacollector.util.Configuration sdcConf = new com.streamsets.datacollector.util.Configuration();
    sdcConf.set(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER, false);
    sdcConf.set(HadoopConfigConstants.LOWERCASE_USER, true);
    final String requestedUser = "Requested";
    final UserGroupInformation proxyUgi = HadoopSecurityUtil.getProxyUser(
        requestedUser,
        "current",
        sdcConf,
        fooUgi
    );

    assertThat(proxyUgi, notNullValue());
    assertThat(proxyUgi.getUserName(), equalTo(requestedUser.toLowerCase()));
  }
}
