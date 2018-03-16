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
   * Return UGI object that should be used for any remote operation.
   *
   * This object will be impersonate according to the configuration. This method is meant to be called once during
   * initialization and it's expected that caller will cache the result for a lifetime of the stage execution.
   */
  public static UserGroupInformation getProxyUser(
    String user,                    // Hadoop user (HDFS User, HBase user, generally the to-be-impersonated user in component's configuration)
    Stage.Context context,          // Stage context object
    UserGroupInformation loginUser, // Login UGI (sdc user)
    List<Stage.ConfigIssue> issues, // Reports errors
    String configGroup,             // Group where "HDFS User" is present
    String configName               // Config name of "HDFS User"
  ) {
    // Should we always impersonate current user?
    String alwaysImpersonateString = Optional
      .ofNullable(context.getConfig(HadoopConfigConstants.IMPERSONATION_ALWAYS_CURRENT_USER))
      .orElse("false");

    // If so, propagate current user to "user" (the one to be impersonated)
    if(Boolean.parseBoolean(alwaysImpersonateString)) {
      if(!StringUtils.isEmpty(user)) {
        issues.add(context.createConfigIssue(configGroup, configName, Errors.HADOOP_00001));
      }
      user = context.getUserContext().getAliasName();
    }

    // If impersonated user is empty, simply return login UGI (no impersonation performed)
    if(StringUtils.isEmpty(user)) {
      return loginUser;
    }

    // Optionally lower case the user name
    String lowercasedString = Optional
      .ofNullable(context.getConfig(HadoopConfigConstants.LOWERCASE_USER))
      .orElse("false");
    if(Boolean.parseBoolean(lowercasedString)) {
      user = user.toLowerCase();
    }
    return UserGroupInformation.createProxyUser(user, loginUser);
  }


}
