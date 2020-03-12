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
import com.streamsets.datacollector.security.usermgnt.FormRealmUsersManager;
import com.streamsets.datacollector.security.usermgnt.User;
import com.streamsets.datacollector.security.usermgnt.UserLineCreator;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.util.resource.Resource;
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
  private SdcHashLoginService hashLoginService;


  FormRealmUsersManager usersManager = null;

  @Override
  public void setLoginService(LoginService loginService) {
    hashLoginService = (SdcHashLoginService) loginService;
  }

  @Override
  public void setRoleMapping(Map<String, Set<String>> roleMapping) {}

  FormRealmUsersManager getUsersManager() {
    FormRealmUsersManager usersManager = null;
    try {
      Resource resource = hashLoginService.getResolvedConfigResource();
      if (hashLoginService != null && resource.exists()) {
        usersManager = new FormRealmUsersManager(UserLineCreator.getMD5Creator(), resource.getFile(), 0);
      }
    } catch (IOException ex) {

    }
    return usersManager;
  }

  @Override
  public List<UserJson> getUsers() {
    FormRealmUsersManager mgr = getUsersManager();
    return (mgr == null) ? Collections.emptyList() :
        mgr.listUsers().stream().map(u -> toUserJson(u)).collect(Collectors.toList());
  }

  @Override
  public List<String> getGroups() {
    FormRealmUsersManager mgr = getUsersManager();
    return (mgr == null) ? Collections.emptyList() : mgr.listGroups();
  }

  @Override
  public UserJson getUser(Principal principal) {
    FormRealmUsersManager mgr = getUsersManager();
    return (mgr == null) ? null : toUserJson(mgr.get(principal.getName()));
  }

  UserJson toUserJson(User user) {
    UserJson userJson = new UserJson();
    userJson.setName(user.getUser());
    userJson.setGroups(user.getGroups());
    userJson.setRoles(user.getRoles());
    return userJson;
  }

}
