/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

@StageDef(description = "Produces facebook feeds", label = "facebook_source"
, version = "1.0")
public class FacebookSource extends BaseSource{

  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the facebook user",
    type = ConfigDef.Type.STRING
  )
  public String username;

  @ConfigDef(
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the facebook user",
    type = ConfigDef.Type.STRING
  )
  public String password;

  public FacebookSource() {

  }
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
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
