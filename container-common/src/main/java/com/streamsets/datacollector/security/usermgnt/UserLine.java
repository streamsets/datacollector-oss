/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security.usermgnt;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class UserLine extends Line {

  public interface Hasher {
    String hash(String user, String password);
  }

  protected static String getUserLineRegex(String mode) {
    Utils.checkNotNull(mode, "mode");
    mode = (mode.isEmpty()) ? "" : mode + ":"; // to handle ClearUserLine
    return "^\\s*([\\w@.]*):\\s*" + mode + "([\\w\\-:]*),user(\\s*$|,.*$)";
  }

  private final String mode;
  private final Hasher hasher;
  private String user;
  private String email;
  private List<String> groups;
  private String hash;
  private List<String> roles;

  protected UserLine(String mode, Hasher hasher, String value) {
    super(Type.USER, null);
    this.mode = Utils.checkNotNull(mode, "mode");
    this.hasher = Utils.checkNotNull(hasher, "hasher");
    parse(Utils.checkNotNull(value, "value"));
  }

  protected UserLine(
      String mode,
      Hasher hasher,
      String user,
      String email,
      List<String> groups,
      List<String> roles, String password
  ) {
    super(Type.USER, null);
    this.mode = Utils.checkNotNull(mode, "mode");
    this.hasher = Utils.checkNotNull(hasher, "hasher");
    this.user = Utils.checkNotNull(user, "user");
    this.email = email;
    this.groups = Utils.checkNotNull(groups, "groups");
    this.roles = Utils.checkNotNull(roles, "roles");
    this.hash = hasher.hash(user, Utils.checkNotNull(password, "password"));
  }

  public String getMode() {
    return mode;
  }

  protected Hasher getHasher() {
    return hasher;
  }

  protected abstract Pattern getPattern();

  private void parse(String value) {
    Matcher matcher = getPattern().matcher(value);
    if (matcher.matches()) {
      user = matcher.group(1);
      hash = matcher.group(2);
      List<String> list = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(matcher.group(3));
      email = list.stream()
          .filter(e -> e.startsWith("email:"))
          .findFirst()
          .map(s -> s.substring("email:".length()))
          .orElse("");
      groups = list.stream()
          .filter(e -> e.startsWith("group:"))
          .map(g -> g.substring("group:".length()))
          .collect(Collectors.toList());
      groups = ImmutableList.<String>builder().add("all").addAll(groups).build();
      roles = list.stream()
          .filter(e -> !(e.startsWith("email:") | e.startsWith("group:")))
          .collect(Collectors.toList());
    }
  }

  public String getUser() {
    return user;
  }

  public String getHash() {
    return hash;
  }

  public UserLine setPassword(String oldPassword, String newPassword) {
    Utils.checkNotNull(oldPassword, "oldPassword");
    Utils.checkNotNull(newPassword, "newPassword");
    if (verifyPassword(oldPassword)) {
      hash = hasher.hash(getUser(), newPassword);
    } else {
      throw new IllegalArgumentException("Invalid old password");
    }
    return this;
  }

  public boolean verifyPassword(String password) {
    Utils.checkNotNull(password, "password");
    String computedHash = hasher.hash(getUser(), password);
    return computedHash.equalsIgnoreCase(hash);  // we are comparing HEX values
  }

  public String resetPassword(long validForMillis) {
    String resetValue = UUID.randomUUID().toString() + ":" + (System.currentTimeMillis() + validForMillis);
    //double hashing to avoid login with the reset token
    this.hash = hasher.hash(getUser(), hasher.hash(getUser(), resetValue));
    return resetValue;
  }

  public void setPasswordFromReset(String resetValue, String newPassword) {
    int expirationIdx = resetValue.lastIndexOf(":");
    if (expirationIdx == -1) {
      throw new IllegalArgumentException("Invalid reset value");
    }
    long expiration = Long.parseLong(resetValue.substring(expirationIdx + 1));
    if (expiration < System.currentTimeMillis()) {
      throw new IllegalArgumentException("Password reset expired");
    }
    String computedHash = hasher.hash(getUser(), hasher.hash(getUser(), resetValue));
    if (!computedHash.equals(hash)) {
      throw new IllegalArgumentException("Invalid reset value");
    }
    this.hash = hasher.hash(getUser(), newPassword);
  }

  public String getEmail() {
    return email;
  }

  public UserLine setEmail(String email) {
    this.email = email;
    return this;
  }

  public List<String> getGroups() {
    return groups;
  }

  public UserLine setGroups(List<String> groups) {
    this.groups = groups.stream().filter(g -> !g.equals("all")).collect(Collectors.toList());
    return this;
  }

  public List<String> getRoles() {
    return roles;
  }

  public UserLine setRoles(List<String> roles) {
    Utils.checkNotNull(roles, "roles");
    this.roles = roles;
    return this;
  }

  @Override
  public String getId() {
    return getUser();
  }

  @Override
  public String getValue() {
    List<String> realmRoles = Stream.concat(
        groups.stream().filter(g -> !g.equals("all")).map(g -> "group:" + g),
        roles.stream()
    )
        .collect(Collectors.toList());
    return String.format(
        "%s: %s:%s,user,%s,%s",
        getUser(),
        getMode(),
        getHash(),
        "email:" + getEmail(),
        realmRoles.stream().collect(Collectors.joining(","))
    );
  }

}
