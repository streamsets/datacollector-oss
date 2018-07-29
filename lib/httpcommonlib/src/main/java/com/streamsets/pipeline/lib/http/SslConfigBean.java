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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.streamsets.pipeline.lib.http.Errors.HTTP_04;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_05;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_29;

/**
 * <p>
 * Use {@link com.streamsets.pipeline.lib.tls.TlsConfigBean} instead going forward.  This class is kept
 * around for now due to test code dependencies.
 * </p>
 */
@Deprecated
public class SslConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Path to Trust Store",
      displayPosition = 10,
      group = "#0"
  )
  public String trustStorePath = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 20,
      group = "#0"
  )
  public CredentialValue trustStorePassword = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Path to Key Store",
      displayPosition = 30,
      group = "#0"
  )
  public String keyStorePath = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 40,
      group = "#0"
  )
  public CredentialValue keyStorePassword = () -> "";

  /**
   * Validates the parameters for this config bean.
   * @param context Stage Context
   * @param groupName Group name this bean is used in
   * @param prefix Prefix to the parameter names (e.g. parent beans)
   * @param issues List of issues to augment
   */
  public void init(Stage.Context context, String groupName, String prefix, List<Stage.ConfigIssue> issues) {
    if (!trustStorePath.isEmpty()) {
      if (Files.notExists(Paths.get(trustStorePath))) {
        issues.add(context.createConfigIssue(groupName, prefix + "trustStorePath", HTTP_04, trustStorePath));
      }

      try {
        if (trustStorePassword.get().isEmpty()) {
          issues.add(context.createConfigIssue(groupName, prefix + "trustStorePassword", HTTP_05));
        }
      } catch (StageException e) {
        issues.add(context.createConfigIssue(groupName, prefix + "trustStorePassword", HTTP_29, e.toString()));
      }

    }

    if (!keyStorePath.isEmpty()) {
      if (Files.notExists(Paths.get(keyStorePath))) {
        issues.add(context.createConfigIssue(groupName, prefix + "keyStorePath", HTTP_04, keyStorePath));
      }

      try {
        if (keyStorePassword.get().isEmpty()) {
          issues.add(context.createConfigIssue(groupName, prefix + "keyStorePassword", HTTP_05));
        }
      } catch (StageException e) {
        issues.add(context.createConfigIssue(groupName, prefix + "keyStorePassword", HTTP_29, e.toString()));
      }
    }
  }
}
