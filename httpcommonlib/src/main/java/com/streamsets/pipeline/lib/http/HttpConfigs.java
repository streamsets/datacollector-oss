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
package com.streamsets.pipeline.lib.http;

import com.streamsets.pipeline.api.Stage;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;

public abstract class HttpConfigs {
  private static final String KEY_STORE_FILE_CONFIG = "keyStoreFile";
  private static final String PORT_CONFIG = "port";

  private final String gropuName;
  private final String configPrefix;

  public HttpConfigs(String gropuName, String configPrefix) {
    this.gropuName = gropuName;
    this.configPrefix = configPrefix;
  }

  public abstract int getPort();

  public abstract int getMaxConcurrentRequests();

  public abstract String getAppId();

  public abstract int getMaxHttpRequestSizeKB();

  public abstract boolean isSslEnabled();

  public abstract boolean isAppIdViaQueryParamAllowed();

  public abstract String getKeyStoreFile();

  public abstract String getKeyStorePassword();

  private String keyStoreFilePath;

  public String getKeyStoreFilePath() {
    return keyStoreFilePath;
  }

  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    validatePort(context, issues);

    if (isSslEnabled()) {
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
    keyStoreFilePath = new File(context.getResourcesDirectory(), getKeyStoreFile()).getAbsolutePath();
    if (!keyStoreFilePath.isEmpty()) {
      File file = new File(keyStoreFilePath);
      if (!file.exists()) {
        issues.add(context.createConfigIssue(gropuName,
            configPrefix + KEY_STORE_FILE_CONFIG,
            HttpServerErrors.HTTP_SERVER_ORIG_07
        ));
      } else {
        if (!file.isFile()) {
          issues.add(context.createConfigIssue(gropuName,
              configPrefix + KEY_STORE_FILE_CONFIG,
              HttpServerErrors.HTTP_SERVER_ORIG_08
          ));
        } else {
          if (!file.canRead()) {
            issues.add(context.createConfigIssue(gropuName,
                configPrefix + KEY_STORE_FILE_CONFIG,
                HttpServerErrors.HTTP_SERVER_ORIG_09
            ));
          } else {
            try {
              KeyStore keystore = KeyStore.getInstance("jks");
              try (InputStream is = new FileInputStream(file)) {
                keystore.load(is, getKeyStorePassword().toCharArray());
              }
            } catch (Exception ex) {
              issues.add(context.createConfigIssue(gropuName,
                  configPrefix + KEY_STORE_FILE_CONFIG,
                  HttpServerErrors.HTTP_SERVER_ORIG_10,
                  ex.toString()
              ));
            }
          }
        }
      }
    } else {
      issues.add(context.createConfigIssue(
          gropuName,
          configPrefix + KEY_STORE_FILE_CONFIG,
          HttpServerErrors.HTTP_SERVER_ORIG_11
      ));
    }
  }

  public void destroy() {
  }

}
