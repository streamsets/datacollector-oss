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
package com.streamsets.datacollector.client.auth;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

public abstract class AbstractAuthentication implements Authentication {
  String username;
  String password;

  @Override
  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public void setDPMBaseURL(String dpmBaseURL) {

  }

  @Override
  public void setFilter(WebTarget webTarget) {

  }

  @Override
  public void setHeader(Invocation.Builder builder, String userAuthToken) {

  }

  @Override
  public String login() {
    return null;
  }

  @Override
  public void logout(String userAuthToken) {

  }
}
