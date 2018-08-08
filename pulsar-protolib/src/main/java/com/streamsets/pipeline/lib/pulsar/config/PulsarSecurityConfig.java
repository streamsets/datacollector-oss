/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.pulsar.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarSecurityConfig {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarSecurityConfig.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "TLS Enabled",
      description = "If enabled the Pulsar URL must be pulsar+ssl://localhost:6651",
      displayPosition = 10,
      group = "SECURITY")
  public boolean tlsEnabled;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "CA Certificate PEM",
      description = "Certification Authority Certificate file path (Put a path relative to resources folder)",
      displayPosition = 30,
      defaultValue = "/pulsar-ca-cert.pem",
      group = "SECURITY",
      dependencies = {
          @Dependency(configName = "tlsEnabled",
              triggeredByValues = "true")
      })
  public String caCertPem;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "TLS Authentication Enabled",
      description = "If enabled communication with Pulsar will be done using TLS with mutual authentication",
      displayPosition = 20,
      group = "SECURITY",
      dependencies = {
          @Dependency(configName = "tlsEnabled",
              triggeredByValues = "true")
      })
  public boolean tlsAuthEnabled;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Client Certificate PEM",
      description = "Client Certificate file path (Put a path relative to resources folder)",
      displayPosition = 40,
      defaultValue = "/pulsar-client-cert.pem",
      group = "SECURITY",
      dependsOn = "tlsEnabled",
      triggeredByValue = "true",
      dependencies = {
          @Dependency(configName = "tlsEnabled",
              triggeredByValues = "true"),
          @Dependency(configName = "tlsAuthEnabled",
              triggeredByValues = "true")
      })
  public String clientCertPem;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Client Key PEM",
      description = "Client Private Key file path (Put a path relative to resources folder)",
      displayPosition = 50,
      defaultValue = "/pulsar-client-key.pem",
      group = "SECURITY",
      dependsOn = "tlsEnabled",
      triggeredByValue = "true",
      dependencies = {
          @Dependency(configName = "tlsEnabled",
              triggeredByValues = "true"),
          @Dependency(configName = "tlsAuthEnabled",
              triggeredByValues = "true")
      })
  public String clientKeyPem;

  private String caCertFileFullPath = null;
  private String clientCertFileFullPath = null;
  private String clientKeyFileFullPath = null;

  private String checkFilePath(
      String resourcePath,
      String configName,
      PulsarErrors emptyPathPulsarError,
      PulsarErrors fileNotExistsPulsarError,
      List<ConfigIssue> issues,
      Stage.Context context
  ) {
    if (resourcePath == null || resourcePath.isEmpty()) {
      LOG.info(emptyPathPulsarError.getMessage());
      issues.add(context.createConfigIssue(PulsarGroups.SECURITY.name(), configName, emptyPathPulsarError));
    } else {
      String resourcesPath = context.getResourcesDirectory();
      resourcesPath = resourcesPath.charAt(resourcesPath.length() - 1) == '/'? resourcesPath : resourcesPath + "/";
      String fullPath = resourcesPath + (resourcePath.charAt(0) == '/'? resourcePath.substring(1) : resourcePath);
      File file = new File(fullPath);
      if (!file.exists()) {
        LOG.info(Utils.format(fileNotExistsPulsarError.getMessage(), resourcePath, fullPath));
        issues.add(context.createConfigIssue(PulsarGroups.SECURITY.name(),
            configName,
            fileNotExistsPulsarError,
            resourcePath,
            fullPath
        ));
      } else {
        return fullPath;
      }
    }

    return null;
  }

  public List<ConfigIssue> init(Stage.Context context) {
    List<ConfigIssue> issues = new ArrayList<>();

    if (tlsEnabled) {
      caCertFileFullPath = checkFilePath(caCertPem, "pulsarConfig.securityConfig.caCertPem", PulsarErrors.PULSAR_11, PulsarErrors
          .PULSAR_12, issues, context);

      if (tlsAuthEnabled) {
        clientCertFileFullPath = checkFilePath(clientCertPem, "pulsarConfig.securityConfig.clientCertPem", PulsarErrors.PULSAR_13,
            PulsarErrors.PULSAR_14, issues, context);

        clientKeyFileFullPath = checkFilePath(clientKeyPem, "pulsarConfig.securityConfig.clientKeyPem", PulsarErrors.PULSAR_15,
            PulsarErrors.PULSAR_16, issues, context);

      }
    }

    return issues;
  }

  public ClientBuilder configurePulsarBuilder(ClientBuilder builder) throws StageException {
    builder.enableTls(tlsEnabled);
    if (tlsEnabled) {
      builder.tlsTrustCertsFilePath(caCertFileFullPath);

      if (tlsAuthEnabled) {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", clientCertFileFullPath);
        authParams.put("tlsKeyFile", clientKeyFileFullPath);

        try {
          builder.authentication(AuthenticationFactory.create(AuthenticationTls.class.getName(), authParams));
        } catch (PulsarClientException.UnsupportedAuthenticationException e) {
          throw new StageException(PulsarErrors.PULSAR_17, e.toString(), e);
        }
      }
    }

    return builder;
  }

}
