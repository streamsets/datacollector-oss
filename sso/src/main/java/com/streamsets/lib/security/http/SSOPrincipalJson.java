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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("squid:S1845")
public class SSOPrincipalJson implements SSOPrincipal {
  private String tokenStr;
  private String issuerUrl;
  private long expires;
  private String principalId;
  private String principalName;
  private String organizationId;
  private String organizationName;
  private String email;
  private Set<String> roles = new HashSet<>();
  private Set<String> groups = new HashSet<>();
  private boolean app;
  private Map<String, String> attributes = new HashMap<>();
  private boolean locked;

  @Override
  public String getTokenStr() {
    return tokenStr;
  }

  public void setTokenStr(String tokenStr) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.tokenStr = tokenStr;
  }

  @Override
  public long getExpires() {
    return expires;
  }

  public void setExpires(long expires) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.expires = expires;
  }

  @Override
  public String getIssuerUrl() {
    return issuerUrl;
  }

  public void setIssuerUrl(String issuerUrl) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.issuerUrl = issuerUrl;
  }

  @Override
  @JsonIgnore
  public String getName() {
    return getPrincipalId();
  }

  @Override
  public String getPrincipalId() {
    return principalId;
  }

  public void setPrincipalId(String userId) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.principalId = userId;
  }

  @Override
  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.principalName = principalName;
  }

  public String getOrganizationId() {
    return organizationId;
  }

  public void setOrganizationId(String organizationId) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.organizationId = organizationId;
  }

  @Override
  public String getOrganizationName() {
    return organizationName;
  }

  public void setOrganizationName(String organizationName) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.organizationName = organizationName;
  }

  @Override
  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.email = email;
  }

  @Override
  public Set<String> getRoles() {
    return roles;
  }

  @Override
  public Set<String> getGroups() {
    return groups;
  }

  @Override
  public boolean isApp() {
    return app;
  }

  public void setApp(boolean app) {
    Utils.checkState(!locked, Utils.formatL("Principal '{}' already locked"));
    this.app = app;
  }

  private final static ThreadLocal<String> REQUEST_IP_ADDRESS_TL = new ThreadLocal<>();

  static void resetRequestIpAddress() {
    REQUEST_IP_ADDRESS_TL.remove();
  }

  void setRequestIpAddress(String ipAddress) {
    REQUEST_IP_ADDRESS_TL.set(ipAddress);
  }

  @Override
  @JsonIgnore
  public String getRequestIpAddress() {
    return REQUEST_IP_ADDRESS_TL.get();
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void lock() {
    locked = true;
    roles = (roles == null) ? Collections.<String>emptySet() : ImmutableSet.copyOf(roles);
    attributes = (attributes == null) ? Collections.<String, String>emptyMap() : ImmutableMap.copyOf(attributes);
    groups = (groups == null) ? Collections.<String>emptySet() : ImmutableSet.copyOf(groups);
  }

  @JsonIgnore
  public boolean isLocked() {
    return locked;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof SSOPrincipalJson) {
      SSOPrincipalJson that = (SSOPrincipalJson) o;
      return getName().equals(that.getName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return (getName() == null) ? 0 : getName().hashCode();
  }

}
