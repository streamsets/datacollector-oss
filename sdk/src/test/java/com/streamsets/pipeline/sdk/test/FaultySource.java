package com.streamsets.pipeline.sdk.test;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;

/**
 * The source has all the required annotations but it does not
 * implement the Source interface or extend the BaseSource abstract class
 */
@StageDef(name = "TwitterSource", description = "Produces twitter feeds", label = "twitter_source"
  , version = "1.0")
public class FaultySource {

  @ConfigDef(
    name = "username",
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.STRING
  )
  private final String username;

  @ConfigDef(
    name = "password",
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the twitter user",
    type = ConfigDef.Type.STRING
  )
  private final String password;

  public FaultySource(String username, String password) {
    this.username = username;
    this.password = password;
  }
}
