/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.external.stage;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

@StageDef(description = "Consumes hdfs feeds", label = "hdfs_target"
, version = "1.3")
public class HdfsTarget implements Target {

  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the hdfs user",
    type = ConfigDef.Type.STRING
  )
  public String username;

  @ConfigDef(
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the hdfs user",
    type = ConfigDef.Type.STRING
  )
  public String password;

  public HdfsTarget() {
  }

  public HdfsTarget(String username, String password) {
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
  public void init(Info info, Context context) throws StageException {

  }

  @Override
  public void destroy() {

  }
}
