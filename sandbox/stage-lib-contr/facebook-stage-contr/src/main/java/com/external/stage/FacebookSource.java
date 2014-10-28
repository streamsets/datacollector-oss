package com.external.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * Created by harikiran on 10/22/14.
 */
@StageDef(name = "FacebookSource", description = "Produces facebook feeds", label = "facebook_source"
, version = "1.0")
public class FacebookSource extends BaseSource{

  @ConfigDef(
    name = "username",
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the facebook user",
    type = ConfigDef.Type.STRING
  )
  private final String username;

  @ConfigDef(
    name = "password",
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the facebook user",
    type = ConfigDef.Type.STRING
  )
  private final String password;

  public FacebookSource(String username, String password) {
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
  public String produce(String lastSourceOffset, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
