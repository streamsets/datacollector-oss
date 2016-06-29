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
package com.streamsets.datacollector.security;

import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.io.IOException;

public class MapRLoginUgiProvider extends LoginUgiProvider {

  public static final String MAPR_USERNAME_PASSWORD_SECURITY_ENABLED_KEY = "maprlogin.password.enabled";
  public static final String MAPR_USERNAME_PASSWORD_SECURITY_ENABLED_DEFAULT = "false";

  @Override
  public UserGroupInformation getLoginUgi(Subject subject) throws IOException {
    // check system property to see if MapR U/P security is enabled
    String maprLoginEnabled = System.getProperty(
        MAPR_USERNAME_PASSWORD_SECURITY_ENABLED_KEY,
        MAPR_USERNAME_PASSWORD_SECURITY_ENABLED_DEFAULT
    );
    boolean isMapRLogin = Boolean.parseBoolean(maprLoginEnabled);

    UserGroupInformation loginUgi;
    if (UserGroupInformation.isSecurityEnabled() && !isMapRLogin) {
      // The code in this block must only be executed in case Kerberos is enabled.
      // MapR implementation of UserGroupInformation.isSecurityEnabled() returns true even if Kerberos is not enabled.
      // System property helps to avoid this code path in such a case
      loginUgi = UserGroupInformation.getUGIFromSubject(subject);
    } else {
      UserGroupInformation.loginUserFromSubject(subject);
      loginUgi = UserGroupInformation.getLoginUser();
    }
    return loginUgi;
  }
}
