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

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * It must always be used in a try (FormRealmUsersManager mgr = ...) {...} block
 */
public class FormRealmUsersManager implements UsersManager, Closeable {
  private final UserLineCreator userLineCreator;
  private final long resetValidityMillis;
  private final DataStore dataStore;
  private final RealmUsers realmUsers;
  private boolean dirty;

  public FormRealmUsersManager(UserLineCreator userLineCreator, File file, long resetValidityMillis)
      throws IOException {
    this.userLineCreator = Utils.checkNotNull(userLineCreator, "userLineCreator");
    this.resetValidityMillis = resetValidityMillis;
    dataStore = new DataStore(Utils.checkNotNull(file, "file"));
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(dataStore.getInputStream()))) {
      realmUsers = RealmUsers.parse(reader);
    }
  }

  @Override
  public void close() throws IOException {
    if (dirty) {
      try (OutputStream os = dataStore.getOutputStream(); Writer writer = new OutputStreamWriter(os)) {
        realmUsers.write(writer); // the writer is being flushed
        dataStore.commit(os);
      } finally {
        dataStore.release();
      }
    }
  }

  @Override
  public String create(String user, String email, List<String> groups, List<String> roles) {
    UserLine userLine = userLineCreator.create(user, email, groups, roles, "dummy");
    String resetPassword = userLine.resetPassword(resetValidityMillis);
    if (realmUsers.add(userLine)) {
      dirty = true;
    } else {
      resetPassword = null;
    }
    return resetPassword;
  }

  @Override
  public User get(String user) {
    User u = null;
    UserLine userLine = realmUsers.find(user);
    if (userLine != null) {
      u = new User(userLine);
    }
    return u;
  }

  @Override
  public String resetPassword(String user) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    String resetPassword = userLine.resetPassword(resetValidityMillis);
    dirty = true;
    return resetPassword;
  }

  @Override
  public void setPasswordFromReset(String user, String resetPassword, String password) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    userLine.setPasswordFromReset(resetPassword, password);
    dirty = true;
  }

  @Override
  public void changePassword(String user, String oldPassword, String newPassword) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    userLine.setPassword(oldPassword, newPassword);
    dirty = true;
  }

  @Override
  public boolean verifyPassword(String user, String password) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    return userLine.verifyPassword(password);
  }

  @Override
  public void delete(String user) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    realmUsers.delete(user);
    dirty = true;
  }

  @Override
  public void update(String user, String email, List<String> groups, List<String> roles) {
    UserLine userLine = realmUsers.find(user);
    Preconditions.checkNotNull(userLine, "User does not exist");
    userLine.setEmail(email);
    userLine.setGroups(groups);
    userLine.setRoles(roles);
    dirty = true;
  }

  @Override
  public List<User> listUsers() {
    return realmUsers.list().stream().map(User::new).collect(Collectors.toList());
  }

  @Override
  public List<String> listGroups() {
    return listUsers().stream()
        .flatMap(u -> u.getGroups().stream())
        .distinct()
        .sorted(String::compareTo)
        .collect(Collectors.toList());
  }

}
