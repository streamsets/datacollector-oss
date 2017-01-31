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
import org.eclipse.jetty.security.LoginService;

import java.security.Principal;
import java.util.Collections;
import java.util.List;

/**
 * Placeholder class for LDAP Login Module
 * // TODO: Fetch users and groups from LDAP server
 */
public class LdapUserGroupManager implements UserGroupManager {

  @Override
  public void setLoginService(LoginService loginService) {
  }

  @Override
  public List<UserJson> getUsers() {
    // TODO: retrieve from LDAP only users belonging to a specified group  (ie sdc-users) for this.
    // this specified group will set in the sdc.properties.
    return Collections.emptyList();
  }

  @Override
  public List<String> getGroups() {
    return Collections.emptyList();
  }

  @Override
  public UserJson getUser(Principal principal) {
    UserJson userJson = new UserJson();
    userJson.setName(principal.getName());
    userJson.setGroups(Collections.<String>emptyList());
    userJson.setRoles(Collections.<String>emptyList());
    return userJson;
  }

}
