package com.external.stage;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseProcessor;

/**
 * Created by harikiran on 10/22/14.
 */
@StageDef(name = "TwitterProcessor", description = "processes twitter feeds", label = "twitter_processor"
, version = "1.0")
public class TwitterProcessor extends BaseProcessor{

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

  public TwitterProcessor(String username, String password) {
    this.username = username;
    this.password = password;
  }



  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws PipelineException {

  }
}

