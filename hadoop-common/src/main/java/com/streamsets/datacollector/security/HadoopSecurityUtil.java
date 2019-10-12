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

import com.streamsets.pipeline.api.Stage;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.server.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HadoopSecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurityUtil.class);

  public static UserGroupInformation getLoginUser(Configuration hdfsConfiguration) throws IOException {
    return LoginUgiProviderFactory.getLoginUgiProvider().getLoginUgi(hdfsConfiguration);
  }

  public static String getDefaultRealm() throws ReflectiveOperationException {
    return KerberosUtil.getDefaultRealm();
  }

  /**
   *
   * Return UGI object that should be used for any remote operation.
   *
   * This object will be impersonate according to the configuration. This method is meant to be called once during
   * initialization and it's expected that caller will cache the result for a lifetime of the stage execution.
   *
   * Delegates to an overload method,
   * {@link #getProxyUser(String, Stage.Context, UserGroupInformation, List, String, String)}
   *
   * @param user Hadoop user (HDFS User, HBase user, generally the to-be-impersonated user in component's configuration)
   * @param context Stage context object
   * @param loginUser login UGI (exx: the "sdc" user)
   * @param issues issues list for reporting errors
   * @param configGroup group where "HDFS User" is present
   * @param configName config name of "HDFS User"
   * @return
   */
  public static UserGroupInformation getProxyUser(
    String user,
    Stage.Context context,
    UserGroupInformation loginUser,
    List<Stage.ConfigIssue> issues,
    String configGroup,
    String configName
  ) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getProxyUser called with user={}, loginUser={}", user, loginUser.toString());
    }
    final String currentUser = getCurrentUser(context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("currentUser calculated as {}", currentUser);
    }
    try {
      return getProxyUser(user, currentUser, context.getConfiguration(),loginUser);
    } catch (IllegalImpersonationException e) {
      LOG.error(
          "{} when {} attempted to impersonate {} (login user: {}): {}",
          e.getClass().getSimpleName(),
          currentUser,
          user,
          loginUser.getUserName(),
          e.getMessage(),
          e
      );
      issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOP_00001));
      return null;
    }
  }

  private static String getCurrentUser(Stage.Context context) {
    if (context != null && context.getUserContext() != null) {
      return context.getUserContext().getAliasName();
    } else {
      return null;
    }
  }

  /**
   *
   * Returns a {@link UserGroupInformation} suitable for running a Hadoop operation as (the proxy-user)
   *
   * @param requestedUser the user that is explicitly being requested, to run as
   * @param currentUser the current user, which is context-dependent
   * @param configuration the SDC configuration
   * @param loginUser the login user (i.e. the ID which stared the container SDC process)
   * @return a {@link UserGroupInformation} representing the proxy user suitable for performing this action
   * @throws IllegalImpersonationException if the requestedUser cannot be impersonated
   */
  public static UserGroupInformation getProxyUser(
      String requestedUser,
      String currentUser,
      com.streamsets.pipeline.api.Configuration configuration,
      UserGroupInformation loginUser
  ) throws IllegalImpersonationException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "getProxyUser called with requestedUser={}, currentUser={}, loginUser={}",
          requestedUser,
          currentUser,
          loginUser.toString()
      );
    }
    // Should we always impersonate current user?
    boolean alwaysImpersonate = configuration.get(
        HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER,
        false
    );
    if (LOG.isDebugEnabled()) {
      LOG.debug("alwaysImpersonate calculated as {}", alwaysImpersonate);
    }

    // If so, propagate current user to "user" (the one to be impersonated)
    if(alwaysImpersonate) {
      if(!StringUtils.isEmpty(requestedUser)) {
        throw new IllegalImpersonationException(requestedUser, currentUser);
      }
      requestedUser = currentUser;
    }

    // If impersonated user is empty, simply return login UGI (no impersonation performed)
    if(StringUtils.isEmpty(requestedUser)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestedUser was empty, thus returning the login UGI of {}", loginUser);
      }
      return loginUser;
    }

    // Optionally lower case the user name
    boolean lowerCase = configuration.get(
        HadoopConfigConstants.LOWERCASE_USER,
        false
    );
    if (LOG.isDebugEnabled()) {
      LOG.debug("lowerCase calculated as {}", lowerCase);
    }
    if(lowerCase) {
      requestedUser = requestedUser.toLowerCase();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "About to create proxy user for requestedUser={}, loginUser={} calculated as {}",
          requestedUser,
          loginUser
      );
    }
    return UserGroupInformation.createProxyUser(requestedUser, loginUser);
  }

}
