/**
 * Copyright 2020 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.rbean.usermgnt;

import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RString;

import java.util.ArrayList;
import java.util.List;

public class RUser extends RBean<RUser> {

  // USER ID
  private RString id = new RString();
  private RString email = new RString();
  private List<RString> groups = new ArrayList<>();
  private List<RString> roles = new ArrayList<>();

  @Override
  public RString getId() {
    return id;
  }

  @Override
  public void setId(RString id) {
    this.id = id;
  }

  public RString getEmail() {
    return email;
  }

  public RUser setEmail(RString email) {
    this.email = email;
    return this;
  }

  public List<RString> getGroups() {
    return groups;
  }

  public RUser setGroups(List<RString> groups) {
    this.groups = groups;
    return this;
  }

  public List<RString> getRoles() {
    return roles;
  }

  public RUser setRoles(List<RString> roles) {
    this.roles = roles;
    return this;
  }

}
