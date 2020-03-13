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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.security.usermgnt.User;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import org.eclipse.jetty.security.LoginService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FileUserGroupManager implements UserGroupManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileUserGroupManager.class);

  private final UsersManager usersManager;

  public FileUserGroupManager(UsersManager usersManager) {
    this.usersManager = usersManager;
  }

  @Override
  public void setLoginService(LoginService loginService) {}

  @Override
  public void setRoleMapping(Map<String, Set<String>> roleMapping) {}

  UsersManager getUsersManager() {
    return usersManager;
  }

  @Override
  public List<UserJson> getUsers() {
    UsersManager mgr = getUsersManager();
    try {
      return (mgr == null) ? Collections.emptyList() :
          mgr.listUsers().stream().map(u -> toUserJson(u)).collect(Collectors.toList());
    } catch (IOException ex) {
      throw new RuntimeException("Cannot happen: " + ex, ex);
    }
  }

  @Override
  public List<String> getGroups() {
    UsersManager mgr = getUsersManager();
    try {
    return (mgr == null) ? Collections.emptyList() : mgr.listGroups();
    } catch (IOException ex) {
      throw new RuntimeException("Cannot happen: " + ex, ex);
    }
  }

  @Override
  public UserJson getUser(Principal principal) {
    UsersManager mgr = getUsersManager();
    try {
    return (mgr == null) ? null : toUserJson(mgr.get(principal.getName()));
    } catch (IOException ex) {
      throw new RuntimeException("Cannot happen: " + ex, ex);
    }
  }

  UserJson toUserJson(User user) {
    UserJson userJson = null;
    if (user != null) {
      userJson = new UserJson();
      userJson.setName(user.getUser());
      userJson.setGroups(user.getGroups());
      userJson.setRoles(user.getRoles());
    }
    return userJson;
  }

}
