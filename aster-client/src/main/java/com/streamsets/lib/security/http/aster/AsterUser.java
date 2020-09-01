/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Bean that models the User information of the engine user as returned by Aster.
 */
public class AsterUser {
  private String name;
  private String org;
  private Set<String> roles = Collections.emptySet();
  private Set<String> groups = Collections.emptySet();
  private String preLoginUrl;

  /**
   * Returns the user name for the engine, typically the user email.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the user name for the engine, typically the user email.
   */
  public AsterUser setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Returns the user Aster org ID.
   */
  public String getOrg() {
    return org;
  }

  /**
   * Sets the user Aster org ID.
   */
  public AsterUser setOrg(String org) {
    this.org = org;
    return this;
  }

  /**
   * Returns the user engine roles.
   */
  public Set<String> getRoles() {
    return roles;
  }

  /**
   * Sets the user engine roles.
   */
  public AsterUser setRoles(Set<String> roles) {
    this.roles = Stream.concat(
        roles.stream().filter(r -> r.startsWith("engine:")).map(r -> r.substring("engine:".length())),
        Stream.of("user")
    ).collect(Collectors.toSet());
    return this;
  }

  /**
   * Returns the user engine groups.
   */
  public Set<String> getGroups() {
    return groups;
  }

  /**
   * Sets the user engine groups.
   */
  public AsterUser setGroups(Set<String> groups) {
    this.groups = groups;
    return this;
  }

  /**
   * Returns the url that triggered the user login.
   */
  public String getPreLoginUrl() {
    return preLoginUrl;
  }

  /**
   * Sets the url that triggered the user login.
   */
  public AsterUser setPreLoginUrl(String preLoginUrl) {
    this.preLoginUrl = preLoginUrl;
    return this;
  }

  /**
   * Convenience method that asserts all properties are set.
   */
  public AsterUser assertValid() {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(org, "org");
    Preconditions.checkNotNull(roles, "roles");
    Preconditions.checkState(!roles.isEmpty(), "user '%s' has no roles", name);
    Preconditions.checkNotNull(groups, "groups");
    return this;
  }

  @Override
  public String toString() {
    return "AsterUser{" +
        "name='" + name + '\'' +
        ", org='" + org + '\'' +
        ", roles=" + roles +
        ", groups=" + groups +
        '}';
  }
}
