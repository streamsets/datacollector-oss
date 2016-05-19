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

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;

import javax.security.auth.Subject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.zookeeper.server.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopSecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurityUtil.class);

  public static UserGroupInformation getLoginUser(Configuration hdfsConfiguration) throws IOException {
    UserGroupInformation loginUgi;
    AccessControlContext accessContext = AccessController.getContext();
    Subject subject = Subject.getSubject(accessContext);
    // As per SDC-2917 doing this avoids deadlock
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      // call some method to force load static block in KerberosName
      KerberosName.hasRulesBeenSet();
    }
    // This should be always out of sync block
    UserGroupInformation.setConfiguration(hdfsConfiguration);
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      if (UserGroupInformation.isSecurityEnabled()) {
        loginUgi = UserGroupInformation.getUGIFromSubject(subject);
      } else {
        UserGroupInformation.loginUserFromSubject(subject);
        loginUgi = UserGroupInformation.getLoginUser();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Subject = {}, Principals = {}, Login UGI = {}", subject, subject == null ? "null" : subject.getPrincipals(),
          loginUgi);
      }
    }
    return loginUgi;
  }

  public static String getDefaultRealm() throws ReflectiveOperationException {
    AccessControlContext accessContext = AccessController.getContext();
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      return KerberosUtil.getDefaultRealm();
    }
  }

  public static UserGroupInformation getProxyUser(String user, UserGroupInformation loginUser) {
    AccessControlContext accessContext = AccessController.getContext();
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      return UserGroupInformation.createProxyUser(user, loginUser);
    }
  }

}
