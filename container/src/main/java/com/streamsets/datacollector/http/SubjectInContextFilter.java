/**
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
import com.streamsets.lib.security.SubjectUtils;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.lib.security.http.SSOPrincipalJson;
import com.streamsets.pipeline.lib.util.ExceptionUtils;

import javax.security.auth.Subject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;

/**
 * Filter that creates a Subject context with the principal of the HTTP request.
 */
class SubjectInContextFilter implements Filter {
  private UserGroupManager userGroupManager;

  protected UserGroupManager getUserGroupManager() {
    return userGroupManager;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    RuntimeInfo runtimeInfo =
        (RuntimeInfo) filterConfig.getServletContext().getAttribute(RuntimeInfoInjector.RUNTIME_INFO);
    if (!runtimeInfo.isDPMEnabled()) {
      userGroupManager =
          (UserGroupManager) filterConfig.getServletContext().getAttribute(UserGroupManagerInjector.USER_GROUP_MANAGER);
    }
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpReq = (HttpServletRequest) req;
    Subject subject;

    Principal principal = httpReq.getUserPrincipal();
    if (principal == null) {
      subject = new Subject();
    } else {
      if (!(principal instanceof SSOPrincipal)) {
        if (getUserGroupManager() == null) {
          throw new RuntimeException("DPM is not enabled and there is no UserGroupManager available");
        }
        UserJson user = getUserGroupManager().getUser(principal);
        if (user == null) {
          user = new UserJson();
          user.setName(principal.getName());
          user.setGroups(ImmutableList.of("all"));
          user.setRoles(ImmutableList.of("user"));
        }
        SSOPrincipalJson ssoPrincipalJson = new SSOPrincipalJson();
        ssoPrincipalJson.setPrincipalId(user.getName());
        ssoPrincipalJson.getGroups().addAll(user.getGroups());
        ssoPrincipalJson.getRoles().addAll(user.getRoles());
        principal = ssoPrincipalJson;
      }
      subject = SubjectUtils.createSubject(principal);
    }

    Subject.doAs(subject, (PrivilegedAction<Void>) () -> {
      try {
        chain.doFilter(req, res);
      } catch (Exception ex) {
        ExceptionUtils.throwUndeclared(ex);
      }
      return null;
    });
  }

  @Override
  public void destroy() {
    userGroupManager = null;
  }
}
