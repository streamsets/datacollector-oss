/**
 * Copyright 2016 StreamSets Inc.
 */
package com.streamsets.lib.security.http;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

public interface SSOUserPrincipal extends Principal {
  String getTokenStr();

  String getTokenId();

  String getIssuerUrl();

  long getExpires();

  // synonyms of getName(), to avoid confusion we should use this value when referring to the uid
  String getUser();

  String getUserFullName();

  String getOrganization();

  String getOrganizationFullName();

  String getEmail();

  Set<String> getRoles();

  Map<String, String> getAttributes();

}
