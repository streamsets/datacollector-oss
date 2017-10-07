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
package com.streamsets.datacollector.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;

public class DefaultLoginUgiProvider extends LoginUgiProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLoginUgiProvider.class);

  @Override
  public UserGroupInformation getLoginUgi(Configuration hdfsConfiguration) throws IOException {
    AccessControlContext accessContext = AccessController.getContext();
    Subject subject = Subject.getSubject(accessContext);
    UserGroupInformation loginUgi;
    UserGroupInformation.setConfiguration(hdfsConfiguration);
    if (UserGroupInformation.isSecurityEnabled()) {
      loginUgi = UserGroupInformation.getUGIFromSubject(subject);
    } else {
      UserGroupInformation.loginUserFromSubject(subject);
      loginUgi = UserGroupInformation.getLoginUser();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Subject = {}, Principals = {}, Login UGI = {}",
          subject,
          subject == null ? "null" : subject.getPrincipals(),
          loginUgi
      );
    }
    return loginUgi;
  }
}
