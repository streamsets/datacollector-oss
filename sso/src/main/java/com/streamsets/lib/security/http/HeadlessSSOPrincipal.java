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

import com.streamsets.pipeline.api.impl.Utils;

import javax.security.auth.Subject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This principal is in the Subject in context when the action is asynchronous from the user.
 * <p/>
 * A recovery principal is to be used then SDC is performing an action automatically and not because
 * of a user request (retry, on SDC restart).
 */
public class HeadlessSSOPrincipal implements SSOPrincipal {
  private final SSOPrincipal principal;
  private final boolean recovery;

  public static HeadlessSSOPrincipal createRecoveryPrincipal(String userId) {
    return new HeadlessSSOPrincipal(userId, Collections.emptySet(), true);
  }

  public HeadlessSSOPrincipal(String userId, Set<String> groups) {
    this(userId, groups, false);
  }

  private HeadlessSSOPrincipal(String userId, Set<String> groups, boolean recovery) {
    Utils.checkNotNull(userId, "userId cannot be NULL");
    Utils.checkNotNull(groups, "groups cannot be NULL");
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setPrincipalId(userId);
    principal.getGroups().addAll(groups);
    principal.lock();
    this.principal = principal;
    this.recovery = recovery;
  }

  @Override
  public String getTokenStr() {
    return principal.getTokenStr();
  }

  @Override
  public String getIssuerUrl() {
    return principal.getIssuerUrl();
  }

  @Override
  public long getExpires() {
    return principal.getExpires();
  }

  @Override
  public String getPrincipalId() {
    return principal.getPrincipalId();
  }

  @Override
  public String getPrincipalName() {
    return principal.getPrincipalName();
  }

  @Override
  public String getOrganizationId() {
    return principal.getOrganizationId();
  }

  @Override
  public String getOrganizationName() {
    return principal.getOrganizationName();
  }

  @Override
  public String getEmail() {
    return principal.getEmail();
  }

  @Override
  public Set<String> getRoles() {
    return principal.getRoles();
  }

  @Override
  public Set<String> getGroups() {
    return principal.getGroups();
  }

  @Override
  public Map<String, String> getAttributes() {
    return principal.getAttributes();
  }

  @Override
  public boolean isApp() {
    return principal.isApp();
  }

  @Override
  public String getRequestIpAddress() {
    return principal.getRequestIpAddress();
  }

  @Override
  public boolean equals(Object another) {
    return principal.equals(another);
  }

  @Override
  public String toString() {
    return principal.toString();
  }

  @Override
  public int hashCode() {
    return principal.hashCode();
  }

  @Override
  public String getName() {
    return principal.getName();
  }

  @Override
  public boolean implies(Subject subject) {
    return principal.implies(subject);
  }

  public boolean isRecovery() {
    return recovery;
  }

}
