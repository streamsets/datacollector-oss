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
package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestHiveConfigBean {

  private static final String IMPERSONATE_CURRENT_USER_KEY = "com.streamsets.pipeline.stage.hive.impersonate.current.user";

  @Mock
  Stage.ConfigIssue configIssue;

  @Test
  public void testJdbcUrlSafeForUser() {
    HiveConfigBean bean = new HiveConfigBean();

    // No parameters
    bean.hiveJDBCUrl = "jdbc:hive2://host:port";
    assertEquals("jdbc:hive2://host:port", bean.jdbcUrlSafeForUser());

    // With parameters
    bean.hiveJDBCUrl = "jdbc:hive2://host:port;password=i-wont-tell-you";
    assertEquals("jdbc:hive2://host:port", bean.jdbcUrlSafeForUser());

    // With database and parameters
    bean.hiveJDBCUrl = "jdbc:hive2://host:port/default;password=i-wont-tell-you";
    assertEquals("jdbc:hive2://host:port/default", bean.jdbcUrlSafeForUser());
  }

  @Test
  public void testValidateHiveImpersonationInvalidURL(){
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    HiveConfigBean bean = new HiveConfigBean();

    Stage.Context context = mock(Stage.Context.class);
    Configuration configuration = new Configuration();
    configuration.set(IMPERSONATE_CURRENT_USER_KEY, true);
    when(context.getConfiguration()).thenReturn(configuration);

    // apache hive jdbc driver
    bean.hiveJDBCUrl = "jdbc:hive2://host:port;hive.server2.proxy.user=sdc";
    when(context.createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl))
        .thenReturn((ConfigIssue) configIssue);

    assertFalse(bean.validateHiveImpersonation(context, "conf", issues));
    verify(context).createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl);

    // cloudera hive jdbc driver
    bean.hiveJDBCUrl = "jdbc:hive2://host:port;DelegationUID=sdc";
    when(context.createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl))
        .thenReturn((ConfigIssue) configIssue);

    assertFalse(bean.validateHiveImpersonation(context, "conf", issues));
    verify(context).createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl);
    verify(context, times(2)).createConfigIssue(anyString(), anyString(), any(), anyString());
  }

  @Test
  public void testValidateHiveImpersonationValidURL(){
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    HiveConfigBean bean = new HiveConfigBean();
    bean.hiveJDBCUrl = "jdbc:hive2://host:port";
    String user = "sdc";

    Stage.Context context = mock(Stage.Context.class);
    Stage.UserContext userContext = mock(Stage.UserContext.class);
    when(context.getUserContext()).thenReturn(userContext);
    when(userContext.getAliasName()).thenReturn(user);
    when(context.createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl))
        .thenReturn((ConfigIssue) configIssue);

    Configuration configuration = new Configuration();
    configuration.set(IMPERSONATE_CURRENT_USER_KEY, true);
    when(context.getConfiguration()).thenReturn(configuration);

    assertTrue(bean.validateHiveImpersonation(context, "conf", issues));
    assertEquals("jdbc:hive2://host:port;hive.server2.proxy.user=sdc", bean.hiveJDBCUrl);
    verify(context, never()).createConfigIssue("HIVE", "conf.hiveJDBCUrl", Errors.HIVE_42, bean.hiveJDBCUrl);
    verify(context, never()).createConfigIssue(anyString(), anyString(), any(), anyString());
  }

  @Test
  public void testImpersonateCurrentUserDisabled(){

    List<Stage.ConfigIssue> issues = new ArrayList<>();
    HiveConfigBean bean = new HiveConfigBean();
    bean.hiveJDBCUrl = "jdbc:hive2://host:port";
    Configuration configuration = new Configuration();

    // hive user impersonation disabled
    configuration.set(IMPERSONATE_CURRENT_USER_KEY, false);

    Stage.Context context = mock(Stage.Context.class);
    when(context.getConfiguration()).thenReturn(configuration);

    assertTrue(bean.validateHiveImpersonation(context, "conf", issues));

    // hive jdbc url unchanged
    assertEquals("jdbc:hive2://host:port", bean.hiveJDBCUrl);
    verify(context, never()).createConfigIssue(anyString(), anyString(), any(), anyString());
  }
}