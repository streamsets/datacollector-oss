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

import java.security.Principal;
import java.util.Map;
import java.util.Set;

public interface SSOPrincipal extends Principal {

  String getTokenStr();

  String getIssuerUrl();

  long getExpires();

  // synonyms of getName(), to avoid confusion we should use this value when referring to the uid
  String getPrincipalId();

  String getPrincipalName();

  String getOrganizationId();

  String getOrganizationName();

  String getEmail();

  Set<String> getRoles();

  Set<String> getGroups();

  Map<String, String> getAttributes();

  boolean isApp();

  String getRequestIpAddress();

}
