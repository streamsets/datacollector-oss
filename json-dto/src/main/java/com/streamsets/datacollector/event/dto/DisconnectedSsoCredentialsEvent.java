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
package com.streamsets.datacollector.event.dto;

import java.util.ArrayList;
import java.util.List;

public class DisconnectedSsoCredentialsEvent implements Event {

  public static class Entry {
    private String userNameSha;
    private String passwordHash;
    private List<String> roles;
    private List<String> groups;

    public Entry() {
      roles = new ArrayList<>();
    }

    public List<String> getGroups() {
      return groups;
    }

    public void setGroups(List<String> groups) {
      this.groups = groups;
    }

    public String getUserNameSha() {
      return userNameSha;
    }

    public void setUserNameSha(String userNameSha) {
      this.userNameSha = userNameSha;
    }

    public String getPasswordHash() {
      return passwordHash;
    }

    public void setPasswordHash(String passwordHash) {
      this.passwordHash = passwordHash;
    }

    public List<String> getRoles() {
      return roles;
    }

    public void setRoles(List<String> roles) {
      this.roles = roles;
    }
  }

  private List<Entry> entries;

  public DisconnectedSsoCredentialsEvent() {
    entries = new ArrayList<>();
  }

  public void setEntries(List<Entry> entries) {
    this.entries = entries;
  }

  public List<Entry> getEntries() {
    return entries;
  }

}
