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
import org.eclipse.jetty.jaas.JAASRole;
import org.eclipse.jetty.jaas.JAASUserPrincipal;
import org.eclipse.jetty.security.LoginService;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LdapUserGroupManager implements UserGroupManager {
  private Map<String, Set<String>> roleMapping;
  private List<String> groupList = new ArrayList<>();

  @Override
  public void setLoginService(LoginService loginService) {
  }

  @Override
  public void setRoleMapping(Map<String, Set<String>> roleMapping) {
    this.roleMapping = roleMapping;
    this.groupList = new ArrayList<>();
    this.groupList.add(ALL_GROUP);
    this.groupList.addAll(roleMapping.keySet());
  }

  @Override
  public List<UserJson> getUsers() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getGroups() {
    return groupList;
  }

  @Override
  public UserJson getUser(Principal userPrincipal) {
    JAASUserPrincipal jaasPrincipal = (JAASUserPrincipal) userPrincipal;
    Set<Principal> principals = jaasPrincipal.getSubject().getPrincipals();
    List<String> roles = new ArrayList<>();
    List<String> groups = new ArrayList<>();
    groups.add(ALL_GROUP);
    for (Principal principal: principals) {
      if (principal instanceof JAASRole) {
        groups.add(principal.getName());
        Set<String> mappedRoles = roleMapping.get(principal.getName());
        if (mappedRoles != null && !mappedRoles.isEmpty()) {
          for (String mappedRole: mappedRoles) {
            if (!roles.contains(mappedRole)) {
              roles.add(mappedRole);
            }
          }
        }
      }
    }
    UserJson userJson = new UserJson();
    userJson.setName(userPrincipal.getName());
    userJson.setGroups(groups);
    userJson.setRoles(roles);
    return userJson;
  }

}
