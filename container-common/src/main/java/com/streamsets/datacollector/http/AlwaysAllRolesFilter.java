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

import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.util.security.Password;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.security.Principal;

public class AlwaysAllRolesFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws
      IOException,
      ServletException {
    request = new HttpServletRequestWrapper((HttpServletRequest) request) {
      @Override
      public boolean isUserInRole(String role) {
        return true;
      }

      // Newer Jersey versions check if there is a principal when the roles permitted list is non-empty.
      // See https://java.net/jira/browse/JERSEY-2908
      @Override
      public Principal getUserPrincipal() {
        return new AbstractLoginService.UserPrincipal("admin", new Password("admin"));
      }
    };
    chain.doFilter(request, response);
  }

  @Override
  public void destroy() {
  }
}
