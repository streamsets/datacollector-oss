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
package com.streamsets.datacollector.security.usermgnt;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TrxUsersManager implements UsersManager {
  private final Lock writeLock;
  private final Lock readLock;
  private final File usersFile;
  private final long expirationMillis;
  private volatile List<User> users;
  private volatile Map<String, User> userMap;
  private volatile List<String> groups;

  public TrxUsersManager(File usersFile) throws IOException {
    this(usersFile, TimeUnit.HOURS.toMillis(2));
  }

  @VisibleForTesting
  public TrxUsersManager(File usersFile, long expirationMillis) throws IOException {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    writeLock = rwLock.writeLock();
    readLock = rwLock.readLock();
    this.usersFile = usersFile;
    this.expirationMillis = expirationMillis;
    reload();
  }

  interface Command<T> {
    T execute(UsersManager mgr) throws IOException;
  }

  <T> T execute(Command<T> command) throws IOException {
    try (
        FormRealmUsersManager usersManager = new FormRealmUsersManager(
            UserLineCreator.getMD5Creator(),
            usersFile,
            expirationMillis
        )
    ) {
      return command.execute(usersManager);
    }
  }

  void reload() throws IOException {
    writeLock.lock();
    try {
      execute(mgr -> {
        users = mgr.listUsers();
        groups = mgr.listGroups();
        return null;
      });
      userMap = users.stream().collect(Collectors.toMap(User::getUser, u -> u));
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String create(String user, String email, List<String> groups, List<String> roles) throws IOException {
    String resetPassword = execute(mgr -> mgr.create(user, email, groups, roles));
    reload();
    return resetPassword;
  }


  @Override
  public User get(String user) {
    readLock.lock();
    try {
      return userMap.get(user);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String resetPassword(String user) throws IOException {
    readLock.lock();
    try {
      return execute(mgr -> mgr.resetPassword(user));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setPasswordFromReset(String user, String resetPassword, String password) throws IOException {
    readLock.lock();
    try {
      execute(mgr -> {
        mgr.setPasswordFromReset(user, resetPassword, password);
        return null;
      });
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void changePassword(String user, String oldPassword, String newPassword) throws IOException {
    readLock.lock();
    try {
      execute(mgr -> {
        mgr.changePassword(user, oldPassword, newPassword);
        return null;
      });
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean verifyPassword(String user, String password) throws IOException {
    readLock.lock();
    try {
      return execute(mgr -> mgr.verifyPassword(user, password));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void delete(String user) throws IOException {
    writeLock.lock();
    try {
      execute(mgr -> {
        mgr.delete(user);
        return null;
      });
      reload();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void update(String user, String email, List<String> groups, List<String> roles) throws IOException {
    writeLock.lock();
    try {
      execute(mgr -> {
        mgr.update(user, email, groups, roles);
        return null;
      });
      reload();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public List<User> listUsers() {
    readLock.lock();
    try {
      return users;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> listGroups() {
    readLock.lock();
    try {
      return groups;
    } finally {
      readLock.unlock();
    }
  }

}
