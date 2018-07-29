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
package com.streamsets.datacollector.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.restapi.configuration.RuntimeInfoInjector;
import com.streamsets.datacollector.restapi.configuration.UserGroupManagerInjector;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.lib.security.http.SSOPrincipal;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;

public class TestGroupsInScopeFilter {

  @Test
  public void testLifecyleNoDPM() throws ServletException {
    FilterConfig config = Mockito.mock(FilterConfig.class);

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);

    UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);

    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(Mockito.eq(UserGroupManagerInjector.USER_GROUP_MANAGER))).thenReturn(userGroupManager);
    Mockito.when(context.getAttribute(Mockito.eq(RuntimeInfoInjector.RUNTIME_INFO))).thenReturn(runtimeInfo);

    Mockito.when(config.getServletContext()).thenReturn(context);

    GroupsInScopeFilter filter = new GroupsInScopeFilter();

    filter.init(config);
    Assert.assertEquals(userGroupManager, filter.getUserGroupManager());

    filter.destroy();
    Assert.assertNull(filter.getUserGroupManager());
  }

  @Test
  public void testLifecyleDPM() throws ServletException {
    FilterConfig config = Mockito.mock(FilterConfig.class);

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);

    UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);

    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(Mockito.eq(UserGroupManagerInjector.USER_GROUP_MANAGER))).thenReturn(userGroupManager);
    Mockito.when(context.getAttribute(Mockito.eq(RuntimeInfoInjector.RUNTIME_INFO))).thenReturn(runtimeInfo);

    Mockito.when(config.getServletContext()).thenReturn(context);

    GroupsInScopeFilter filter = new GroupsInScopeFilter();

    filter.init(config);
    Assert.assertNull(filter.getUserGroupManager());

    filter.destroy();
    Assert.assertNull(filter.getUserGroupManager());
  }

  @Test
  public void testNoPrincipalInRequest() throws ServletException, IOException {
    GroupsInScopeFilter filter = new GroupsInScopeFilter();
    filter = Mockito.spy(filter);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        Assert.assertTrue(GroupsInScope.isUserGroupInScope("all"));
      }
    };

    chain = Mockito.spy(chain);

    filter.doFilter(req, res, chain);
    Mockito.verify(chain, Mockito.times(1)).doFilter(Mockito.eq(req), Mockito.eq(res));
  }

  @Test
  public void testDoFilterSSOPrincipalInRequest() throws ServletException, IOException {
    GroupsInScopeFilter filter = new GroupsInScopeFilter();

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    SSOPrincipal principal = Mockito.mock(SSOPrincipal.class);
    Mockito.when(principal.getGroups()).thenReturn(ImmutableSet.of("all", "g"));
    Mockito.when(req.getUserPrincipal()).thenReturn(principal);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        Assert.assertTrue(GroupsInScope.isUserGroupInScope("all"));
        Assert.assertTrue(GroupsInScope.isUserGroupInScope("g"));
        Assert.assertFalse(GroupsInScope.isUserGroupInScope("x"));
      }
    };

    chain = Mockito.spy(chain);

    filter.doFilter(req, res, chain);
    Mockito.verify(chain, Mockito.times(1)).doFilter(Mockito.eq(req), Mockito.eq(res));
  }

  @Test
  public void testDoFilterWithUserGroupManagerWithMissingUser() throws ServletException, IOException {
    GroupsInScopeFilter filter = new GroupsInScopeFilter();
    filter = Mockito.spy(filter);

    UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);
    Mockito.doReturn(userGroupManager).when(filter).getUserGroupManager();

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Principal principal = Mockito.mock(Principal.class);
    Mockito.when(principal.getName()).thenReturn("user");
    Mockito.when(req.getUserPrincipal()).thenReturn(principal);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        Assert.assertTrue(GroupsInScope.isUserGroupInScope("all"));
        Assert.assertFalse(GroupsInScope.isUserGroupInScope("x"));
      }
    };

    chain = Mockito.spy(chain);

    filter.doFilter(req, res, chain);
    Mockito.verify(chain, Mockito.times(1)).doFilter(Mockito.eq(req), Mockito.eq(res));

    filter.destroy();
  }

  @Test
  public void testDoFilterWithUserGroupManagerWithUser() throws ServletException, IOException {
    GroupsInScopeFilter filter = new GroupsInScopeFilter();
    filter = Mockito.spy(filter);

    Principal principal = Mockito.mock(Principal.class);
    Mockito.when(principal.getName()).thenReturn("user");

    UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);
    UserJson user = new UserJson();
    user.setName("user");
    user.setGroups(ImmutableList.of("g"));
    user.setRoles(ImmutableList.of("r"));
    Mockito.when(userGroupManager.getUser(Mockito.eq(principal))).thenReturn(user);
    Mockito.doReturn(userGroupManager).when(filter).getUserGroupManager();

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getUserPrincipal()).thenReturn(principal);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        Assert.assertTrue(GroupsInScope.isUserGroupInScope("g"));
        Assert.assertFalse(GroupsInScope.isUserGroupInScope("x"));
      }
    };

    chain = Mockito.spy(chain);

    filter.doFilter(req, res, chain);
    Mockito.verify(chain, Mockito.times(1)).doFilter(Mockito.eq(req), Mockito.eq(res));

    filter.destroy();
  }

}
