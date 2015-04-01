/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;


import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.Collections;
import java.util.List;

@GenerateResourceBundle
@StageDef(description = "Consumes twitter feeds", label = "twitter_target"
, version = "1.3")
public class TwitterTarget implements Target {

  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.STRING
  )
  public String username;

  @ConfigDef(
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the twitter user",
    type = ConfigDef.Type.STRING
  )
  public String password;

  public TwitterTarget() {
  }

  public TwitterTarget(String username, String password) {
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
  public void write(Batch batch) throws StageException {

  }

  @Override
  public List<ConfigIssue> validateConfigs(Info info, Context context) {
    return Collections.emptyList();
  }

  @Override
  public void init(Info info, Context context) throws StageException {

  }

  @Override
  public void destroy() {

  }
}
