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

import com.streamsets.datacollector.http.SdcHashLoginService;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class FileUserGroupManager implements UserGroupManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileUserGroupManager.class);
  private static final String GROUP_PREFIX = "group:";
  private static final String USER_ROLE = "user";
  private SdcHashLoginService hashLoginService;
  private Map<String, UserJson> usersMap;
  private List<UserJson> userList;
  private List<String> groupList;
  private volatile boolean initialized = false;

  @Override
  public void setLoginService(LoginService loginService) {
    hashLoginService = (SdcHashLoginService) loginService;
  }

  @Override
  public void setRoleMapping(Map<String, Set<String>> roleMapping) {}

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
  public UserJson getUser(Principal principal) {
    initialize();
    return usersMap.get(principal.getName());
  }

  private void initialize() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          try {
            fetchUserAndGroupList();
          } catch (IOException e) {
            LOG.warn(Utils.format("Exception when fetching users and groups: {}", e.getMessage()));
          }
          initialized = true;
        }
      }
    }
  }

  private void fetchUserAndGroupList() throws IOException {
    userList = new ArrayList<>();
    usersMap = new HashMap<>();
    groupList = new ArrayList<>();
    groupList.add(ALL_GROUP);
    if (hashLoginService != null) {
      Properties properties = new Properties();
      if ( hashLoginService.getResolvedConfigResource().exists()) {
        properties.load(hashLoginService.getResolvedConfigResource().getInputStream());
      }

      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        String userName = ((String)entry.getKey()).trim();
        UserJson user = new UserJson();
        user.setName(userName);
        List<String> roles = new ArrayList<>();
        List<String> groups = new ArrayList<>();
        groups.add(ALL_GROUP);
        UserIdentity userIdentity = hashLoginService.getUserIdentity(userName);
        Set<Principal> principals = userIdentity.getSubject().getPrincipals();
        for (Principal principal: principals) {
          if (principal instanceof AbstractLoginService.RolePrincipal) {
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
