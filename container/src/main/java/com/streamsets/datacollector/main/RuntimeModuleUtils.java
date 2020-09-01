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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.security.usermgnt.NoOpUsersManager;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.IOException;

public class RuntimeModuleUtils {

  public static UsersManager provideUsersManager(RuntimeInfo runtimeInfo, Configuration configuration) {
    String loginModule = configuration.get(
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE,
        WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT
    );
    UsersManager usersManager = new NoOpUsersManager();
    if (!runtimeInfo.isDPMEnabled() && WebServerTask.FILE.equals(loginModule)) {
      String auth = configuration.get(WebServerTask.AUTHENTICATION_KEY, WebServerTask.AUTHENTICATION_DEFAULT);
      switch (auth) {
        case "form":
        case "digest":
        case "basic":
          try {
            usersManager = new TrxUsersManager(new File(runtimeInfo.getConfigDir(), auth + "-realm.properties"));
          } catch (IOException ex) {
            throw new RuntimeException(ex.toString(), ex);
          }
          break;
        case "aster":
        case "none":
          break;
        default:
          throw new RuntimeException(Utils.format("Invalid Authentication Login Module '{}', must be one of '{}'",
              auth, WebServerTask.LOGIN_MODULES));
      }
    }
    return usersManager;
  }

}
