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

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

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

    when(context.getConfig(eq(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER))).thenReturn("true");
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

    when(context.getConfig(anyString())).thenReturn("true");
    when(context.getUserContext()).thenReturn(userContext);

    UserGroupInformation ugi = HadoopSecurityUtil.getProxyUser(
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

    when(context.getConfig(eq(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER))).thenReturn("true");
    when(context.getConfig(eq(HadoopConfigConstants.LOWERCASE_USER))).thenReturn("true");
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
}
