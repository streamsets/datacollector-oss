/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ConfigDef;

public class CredentialsConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "true",
    label = "Use Credentials",
    displayPosition = 70,
    group = "#0"
  )
  public boolean useCredentials;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    dependsOn = "useCredentials",
    triggeredByValue = "true",
    label = "Username",
    displayPosition = 10,
    group = "CREDENTIALS"
  )
  public String username;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    dependsOn = "useCredentials",
    triggeredByValue = "true",
    label = "Password",
    displayPosition = 20,
    group = "CREDENTIALS"
  )
  public String password;
}
