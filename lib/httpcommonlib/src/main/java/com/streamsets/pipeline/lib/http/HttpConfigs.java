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
package com.streamsets.pipeline.lib.http;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public abstract class HttpConfigs {
  private static final String PORT_CONFIG = "port";

  private final String gropuName;
  private final String configPrefix;

  public HttpConfigs(String gropuName, String configPrefix) {
    this.gropuName = gropuName;
    this.configPrefix = configPrefix;
  }

  public abstract int getPort();

  public abstract int getMaxConcurrentRequests();

  public abstract CredentialValue getAppId();

  public abstract int getMaxHttpRequestSizeKB();

  public abstract boolean isTlsEnabled();

  public abstract boolean isAppIdViaQueryParamAllowed();

  public abstract TlsConfigBean getTlsConfigBean();

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    validatePort(context, issues);

    if (isTlsEnabled()) {
      validateSecurity(context, issues);
    }

    return issues;
  }

  void validatePort(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (getPort() < 1 || getPort() > 65535) {
      issues.add(context.createConfigIssue(gropuName, configPrefix + PORT_CONFIG, HttpServerErrors.HTTP_SERVER_ORIG_00));

    } else {
      try (ServerSocket ss = new ServerSocket(getPort())) {
      } catch (Exception ex) {
        issues.add(context.createConfigIssue(gropuName,
            configPrefix + PORT_CONFIG,
            HttpServerErrors.HTTP_SERVER_ORIG_01,
            ex.toString()
        ));

      }
    }
  }

  void validateSecurity(Stage.Context context, List<Stage.ConfigIssue> issues) {
    TlsConfigBean tlsConfigBean = getTlsConfigBean();
    tlsConfigBean.init(context, "TLS", configPrefix+"tlsConfigBean.", issues);
  }

  public void destroy() {
  }

}
