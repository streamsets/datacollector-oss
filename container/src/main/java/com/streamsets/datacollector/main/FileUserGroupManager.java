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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.restapi.bean.UserJson;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class FileUserGroupManager implements UserGroupManager {
  private static final String GROUP_PREFIX = "group:";
  private static final String USER_ROLE = "user";
  private static final String ALL_GROUP = "all";
  private HashLoginService hashLoginService;
  private Map<String, UserJson> usersMap;
  private List<UserJson> userList;
  private List<String> groupList;
  private volatile boolean initialized = false;

  @Override
  public void setLoginService(LoginService loginService) {
    hashLoginService = (HashLoginService) loginService;
  }

  @Override
  public List<UserJson> getUsers() {
    initialize();
    return userList;
  }

  @Override
  public List<String> getGroups() {
    initialize();
    return groupList;
  }

  @Override
  public UserJson getUser(final String userName) {
    initialize();
    return usersMap.get(userName);
  }

  private void initialize() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          fetchUserAndGroupList();
          initialized = true;
        }
      }
    }
  }

  private void fetchUserAndGroupList() {
    userList = new ArrayList<>();
    usersMap = new HashMap<>();
    groupList = new ArrayList<>();
    if (hashLoginService != null) {
      ConcurrentMap<String, UserIdentity> userIdentityMap = hashLoginService.getUsers();
      for (String userName: userIdentityMap.keySet()) {
        UserJson user = new UserJson();
        user.setName(userName);
        List<String> roles = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        groups.add(ALL_GROUP);
        UserIdentity userIdentity = userIdentityMap.get(userName);
        Set<Principal> principals = userIdentity.getSubject().getPrincipals();
        for (Principal principal: principals) {
          if (principal instanceof MappedLoginService.RolePrincipal) {
            if (principal.getName().startsWith(GROUP_PREFIX)) {
              String groupName = principal.getName().replace(GROUP_PREFIX, "");
              if (!groups.contains(groupName)) {
                groups.add(groupName);
              }
              if (!groupList.contains(groupName)) {
                groupList.add(groupName);
              }
            } else if (!roles.contains(principal.getName()) && !USER_ROLE.equals(principal.getName())) {
              roles.add(principal.getName());
            }
          }
        }
        user.setRoles(roles);
        user.setGroups(groups);
        userList.add(user);
        usersMap.put(userName, user);
      }
    }
  }


}
