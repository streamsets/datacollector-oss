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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("squid:S1845")
public class SSOUserPrincipalImpl implements SSOUserPrincipal {
  private static final Logger LOG = LoggerFactory.getLogger(SSOUserPrincipalImpl.class);

  public static final String TOKEN_ID = "_tokenId";
  public static final String EXPIRES = "_expires";
  public static final String ISSUER_URL = "_issuerUrl";
  public static final String USER_ID = "_userId";
  public static final String ORG_ID = "_orgId";
  public static final String USER_NAME = "_userName";
  public static final String ORG_NAME = "_orgName";
  public static final String USER_EMAIL = "_userEmail";
  public static final String ROLES = "_userRoles";

  private static final Set<String> REQUIRED_PROPERTIES = ImmutableSet.<String>builder().add(
      TOKEN_ID,
      EXPIRES,
      ISSUER_URL,
      USER_ID,
      ORG_ID,
      USER_NAME,
      ORG_NAME,
      USER_EMAIL,
      USER_ID,
      ROLES
  ).build();

  private final String tokenId;
  private final long expires;
  private final String issuerUrl;
  private final String userId;
  private final String orgId;
  private final String name;
  private final String orgName;
  private final String email;
  private final Set<String> roles;
  private final Map<String, String> attributes;
  private final String tokenStr;

  public SSOUserPrincipalImpl(
      String tokenId,
      long expires,
      String issuerUrl,
      String userId,
      String orgId,
      String name,
      String orgName,
      String email,
      Set<String> roles,
      Map<String, String> attributes
  ) {
    this(tokenId, expires, issuerUrl, userId, orgId, name, orgName, email, roles, attributes, null);
  }

  public SSOUserPrincipalImpl(
      String tokenId,
      long expires,
      String issuerUrl,
      String userId,
      String orgId,
      String name,
      String orgName,
      String email,
      Set<String> roles,
      Map<String, String> attributes,
      String tokenStr
  ) {
    this.tokenId = tokenId;
    this.expires = expires;
    this.issuerUrl = issuerUrl;
    this.userId = userId;
    this.orgId = orgId;
    this.name = name;
    this.orgName = orgName;
    this.email = email;
    this.roles = roles;
    this.attributes = vetoAttributes(attributes);
    this.tokenStr = tokenStr;
  }

  static Map<String, String> vetoAttributes(Map<String, String> attributes) {
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      Utils.checkArgument(
          !REQUIRED_PROPERTIES.contains(entry.getKey()),
          Utils.formatL("Attributes cannot use " + "reserved keys, using '{}'", entry.getKey())
      );
    }
    return attributes;
  }

  @SuppressWarnings({"unchecked", "squid:S1226"})
  public static SSOUserPrincipal fromMap(String tokenStr, Map<String, ?> map) {
    Utils.checkNotNull(tokenStr, "tokenStr");
    Utils.checkNotNull(map, "map");

    map = new HashMap<>(map);
    validateAllString(map);
    validateData(tokenStr, map);
    String tokenId = (String) map.remove(TOKEN_ID);
    long expires = Long.parseLong((String)map.remove(EXPIRES));
    String issuerUrl = (String) map.remove(ISSUER_URL);
    String userId = (String) map.remove(USER_ID);
    String orgId = (String) map.remove(ORG_ID);
    String name = (String) map.remove(USER_NAME);
    String orgName = (String) map.remove(ORG_NAME);
    String email = (String) map.remove(USER_EMAIL);
    Set<String> roles = ImmutableSet.copyOf(Splitter.on(",").trimResults().split((String) map.remove(ROLES)));
    Map<String, String> attributes = vetoAttributes((Map<String, String>)map);
    return new SSOUserPrincipalImpl(
        tokenId,
        expires,
        issuerUrl,
        userId,
        orgId,
        name,
        orgName,
        email,
        roles,
        attributes,
        tokenStr
    );
  }

  public static Map<String, String> toMap(SSOUserPrincipal principal) {
    Map<String, String> map = new HashMap<>();
    map.put(TOKEN_ID, principal.getTokenId());
    map.put(EXPIRES, Long.toString(principal.getExpires()));
    map.put(ISSUER_URL, principal.getIssuerUrl());
    map.put(USER_ID, principal.getName());
    map.put(ORG_ID, principal.getOrganization());
    map.put(USER_NAME, principal.getUserFullName());
    map.put(ORG_NAME, principal.getOrganizationFullName());
    map.put(USER_EMAIL, principal.getEmail());
    map.put(ROLES, Joiner.on(",").join(principal.getRoles()));
    map.putAll(principal.getAttributes());
    return map;

  }

  static void validateAllString(Map<String, ?> map) {
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      Utils.checkArgument(
          entry.getValue() == null || entry.getValue() instanceof String,
          Utils.formatL("SSO token '{}' has non-string values", map)
      );
    }
  }

  @SuppressWarnings("squid:S1301")
  static void validateData(String tokenStr, Map<String, ?> map) {
    if (!map.keySet().containsAll(REQUIRED_PROPERTIES)) {
      Set<String> missing = new HashSet<>(REQUIRED_PROPERTIES);
      missing.removeAll(map.keySet());
      LOG.warn("SSO token '{}' misses the following properties: {}", tokenStr, missing);
      throw new IllegalArgumentException("Token payload does not include all required properties");
    }
  }

  @Override
  public String getTokenStr() {
    return tokenStr;
  }

  @Override
  public String getTokenId() {
    return tokenId;
  }

  @Override
  public String getIssuerUrl() {
    return issuerUrl;
  }

  @Override
  public long getExpires() {
    return expires;
  }

  @Override
  public String getName() {
    return userId;
  }

  @Override
  public String getUser() {
    return getName();
  }

  @Override
  public String getUserFullName() {
    return name;
  }

  @Override
  public String getOrganization() {
    return orgId;
  }

  @Override
  public String getOrganizationFullName() {
    return orgName;
  }

  @Override
  public String getEmail() {
    return email;
  }

  @Override
  public Set<String> getRoles() {
    return roles;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SSOUserPrincipalImpl that = (SSOUserPrincipalImpl) o;

    return name.equals(that.name);

  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
