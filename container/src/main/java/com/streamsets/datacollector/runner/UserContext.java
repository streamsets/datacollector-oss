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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Stage;

public class UserContext implements Stage.UserContext {

  private final String user;
  private boolean isDpmEnabled;
  private boolean isAliasEnabled;

  public UserContext(String user, boolean isDpmEnabled, boolean isAliasEnabled) {
    this.user = user;
    this.isDpmEnabled = isDpmEnabled;
    this.isAliasEnabled = isAliasEnabled;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getAliasName() {
    String userId = getUser();
    if (isDpmEnabled && isAliasEnabled) {
      return userId.substring(0, userId.indexOf("@"));
    } else {
      return userId;
    }
  }


}
