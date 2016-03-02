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

import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSOUserToken {
  public static final String TOKEN_ID = "_tokenId";
  public static final String EXPIRES = "_expires";
  public static final String ISSUER_URL = "_issuerUrl";
  public static final String USER_ID = "_userId";
  public static final String ORG_ID = "_orgId";
  public static final String USER_NAME = "_name";
  public static final String ROLES = "_roles";

  private Map<String, ?> map;
  private String rawToken;

  @SuppressWarnings("unchecked")
  public SSOUserToken(Map map) {
    this.map = map;
  }

  public void setRawToken(String rawToken) {
    Utils.checkState(this.rawToken == null, Utils.formatL("Token ID '{}', raw token already set", getId()));
    this.rawToken = rawToken;
  }

  public String getRawToken() {
    return rawToken;
  }


  public String getId() {
    return (String) map.get(TOKEN_ID);
  }

  public long getExpires() {
    return ((Number)map.get(EXPIRES)).longValue();
  }

  public String getUserId() {
    return (String) map.get(USER_ID);
  }

  public String getUserName() {
    return (String) map.get(USER_NAME);
  }

  public String getOrganizationId() {
    return (String) map.get(ORG_ID);
  }

  public String getIssuerUrl() {
    return (String) map.get(ISSUER_URL);
  }

  @SuppressWarnings("unchecked")
  public List<String> getRoles() {
    return (List<String>) map.get(ROLES);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getUserAttributes() {
    Map<String, Object> attributes = new HashMap<>();
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      if (!entry.getKey().startsWith("_")) {
        attributes.put(entry.getKey(), entry.getValue());
      }
    }
    return attributes;
  }

}
