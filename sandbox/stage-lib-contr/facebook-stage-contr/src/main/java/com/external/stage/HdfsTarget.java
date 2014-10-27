package com.external.stage;

import com.streamsets.pipeline.api.*;

/**
 * Created by harikiran on 10/22/14.
 */
@StageDef(name = "HdfsTarget", description = "Consumes hdfs feeds", label = "hdfs_target"
, version = "1.3")
public class HdfsTarget implements Target {

  @ConfigDef(
    name = "username",
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the hdfs user",
    type = ConfigDef.Type.STRING
  )
  private final String username;

  @ConfigDef(
    name = "password",
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the hdfs user",
    type = ConfigDef.Type.STRING
  )
  private final String password;

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
