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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DisconnectedSecurityInfo {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static class Entry {
    private String userNameSha;
    private String passwordHash;
    private List<String> roles;
    private List<String> groups;

    public Entry() {
      roles = new ArrayList<>();
      groups = new ArrayList<>();
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

    public List<String> getGroups() {
      return groups;
    }
  }

  private final Map<String, Entry> entries;

  public DisconnectedSecurityInfo() {
    entries = new HashMap<>();
  }

  public List<Entry> getEntries() {
    return new ArrayList<>(entries.values());
  }

  public void setEntries(List<Entry> entries) {
    this.entries.clear();
    for (Entry entry : entries) {
      this.entries.put(entry.getUserNameSha(), entry);
    }
  }

  public Entry getEntry(String userName) {
    return entries.get(DigestUtils.sha256Hex(userName));
  }

  public void addEntry(String userName, String passwordHash, List<String> roles, List<String> groups) {
    Entry entry = new Entry();
    entry.setUserNameSha(DigestUtils.sha256Hex(userName));
    entry.setPasswordHash(passwordHash);
    entry.getRoles().addAll(roles);
    entry.getGroups().addAll(groups);
    entries.put(entry.getUserNameSha(), entry);
  }

  public String toJsonString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (IOException ex) {
      throw new RuntimeException("Should never happen: " + ex.toString(), ex);
    }
  }

  public void toJsonFile(File file) throws IOException {
    OBJECT_MAPPER.writeValue(file, this);
  }

  public static DisconnectedSecurityInfo fromJsonString(String json) throws IOException {
    return OBJECT_MAPPER.readValue(json, DisconnectedSecurityInfo.class);
  }

  public static DisconnectedSecurityInfo fromJsonFile(File file) throws IOException {
    return OBJECT_MAPPER.readValue(file, DisconnectedSecurityInfo.class);
  }

}
