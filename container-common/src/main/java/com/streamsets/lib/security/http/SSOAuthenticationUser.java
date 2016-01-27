/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.security.Principal;
import java.util.Collections;

public class SSOAuthenticationUser implements Authentication.User, Principal {
  private final SSOUserToken userToken;
  private final String id;
  private transient boolean valid;

  private SSOAuthenticationUser(String id) {
    userToken = null;
    this.id = id;
    valid = false;
  }

  public SSOAuthenticationUser(final SSOUserToken userToken) {
    Utils.checkNotNull(userToken, "clientUserToken");
    this.userToken = userToken;
    this.id = userToken.getId();
    valid = true;
  }

  @Override
  public String getName() {
    return userToken.getUserId();
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
        return new Subject(true, ImmutableSet.of(SSOAuthenticationUser.this), Collections.emptySet(), Collections
            .emptySet());
      }

      @Override
      public Principal getUserPrincipal() {
        return SSOAuthenticationUser.this;
      }

      @Override
      public boolean isUserInRole(String s, Scope scope) {
        return userToken.getRoles().contains(s);
      }
    };
  }

  @Override
  public boolean isUserInRole(UserIdentity.Scope scope, String s) {
    return userToken.getRoles().contains(s);
  }

  @Override
  public void logout() {
    valid = false;
  }

  public SSOUserToken getToken() {
    return userToken;
  }

  public boolean isValid() {
    return valid && System.currentTimeMillis() < userToken.getExpires();
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

  public static SSOAuthenticationUser createForInvalidation(String id) {
    return new SSOAuthenticationUser(id);
  }

}
