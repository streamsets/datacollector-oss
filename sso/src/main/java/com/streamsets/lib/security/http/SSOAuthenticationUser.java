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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import java.security.Principal;
import java.util.Collections;

public class SSOAuthenticationUser implements Authentication.User {
  private final SSOPrincipal principal;
  private final String id;
  private volatile boolean valid;
  private long validationTime;

  public SSOAuthenticationUser(final SSOPrincipal principal) {
    Utils.checkNotNull(principal, "principal");
    this.principal = principal;
    this.id = Utils.checkNotNull(principal.getTokenStr(), "principal.tokenStr");
    valid = true;
    validationTime = System.currentTimeMillis();
  }

  @Override
  public String getAuthMethod() {
    return SSOConstants.AUTHENTICATION_METHOD;
  }

  @Override
  public UserIdentity getUserIdentity() {
    return new UserIdentity() {
      @Override
      public Subject getSubject() {
        return new Subject(true, ImmutableSet.of(principal), Collections.emptySet(), Collections.emptySet());
      }

      @Override
      public Principal getUserPrincipal() {
        return principal;
      }

      @Override
      public boolean isUserInRole(String s, Scope scope) {
        return principal.getRoles().contains(s);
      }
    };
  }

  @Override
  public boolean isUserInRole(UserIdentity.Scope scope, String s) {
    return principal.getRoles().contains(s);
  }

  @Override
  public void logout() {
    logout(null);
  }

  public SSOPrincipal getSSOUserPrincipal() {
    return principal;
  }

  public boolean isValid() {
    return valid && System.currentTimeMillis() < Math.abs(principal.getExpires());
  }

  public long getValidationTime() {
    return validationTime;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    boolean eq = false;
    if (obj != null && obj instanceof SSOAuthenticationUser) {
      eq = id.equals(((SSOAuthenticationUser) obj).id);
    }
    return eq;
  }

  @Override
  public Authentication logout(ServletRequest request) {
    valid = false;
    return null;
  }
}
