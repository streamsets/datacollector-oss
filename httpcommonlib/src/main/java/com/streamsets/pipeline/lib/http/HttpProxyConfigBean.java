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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ConfigDef;

import java.util.List;

public class HttpProxyConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Proxy URI",
      dependsOn = "useProxy^",
      triggeredByValue = "true",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public String uri = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      dependsOn = "useProxy^",
      triggeredByValue = "true",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      dependsOn = "useProxy^",
      triggeredByValue = "true",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public CredentialValue password = () -> "";

  public String resolveUsername(
    Stage.Context context,
    String groupName,
    String prefix,
    List<Stage.ConfigIssue> issues
  ) {
    return resolveCredential(
      username,
      "username",
      context,
      groupName,
      prefix,
      issues
    );
  }

  public String resolvePassword(
    Stage.Context context,
    String groupName,
    String prefix,
    List<Stage.ConfigIssue> issues
  ) {
    return resolveCredential(
      password,
      "password",
      context,
      groupName,
      prefix,
      issues
    );
  }

  private String resolveCredential(
    CredentialValue credentialValue,
    String property,
    Stage.Context context,
    String groupName,
    String prefix,
    List<Stage.ConfigIssue> issues
  ) {
    if(credentialValue == null) {
      return null;
    }

    try {
      return credentialValue.get();
    } catch (StageException e) {
      issues.add(context.createConfigIssue(
        groupName,
        prefix + property,
        Errors.HTTP_29,
        property,
        e.toString()));
      return null;
    }
  }
}
