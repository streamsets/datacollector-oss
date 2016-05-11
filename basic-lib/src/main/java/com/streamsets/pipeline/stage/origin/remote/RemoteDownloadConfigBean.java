/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class RemoteDownloadConfigBean {

  @ConfigDefBean(groups = "REMOTE")
  public BasicConfig basic = new BasicConfig();

  @ConfigDefBean(groups = "REMOTE")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      description = "Specify the SFTP/FTP URL",
      displayPosition = 10,
      group = "REMOTE"
  )
  public String remoteAddress;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Path relative to User Home Directory",
      description = "If checked, the path is resolved relative to the logged in user's home directory",
      displayPosition = 20,
      group = "REMOTE"
  )
  public boolean userDirIsRoot = true;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Username",
      description = "Username to use to login to the remote server",
      group = "CREDENTIALS"
  )
  public String username;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Password",
      description = "Password to use to login to the remote server",
      group = "CREDENTIALS"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Strict Host Checking",
      description = "If enabled, this client will only connect to the host if the host is in the known hosts file",
      group = "CREDENTIALS"
  )
  public boolean strictHostChecking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Known Hosts file",
      description = "Full path to the file that lists the host keys of all known hosts." +
          "This must be specified if the strict host checking is enabled",
      group = "CREDENTIALS",
      dependsOn = "strictHostChecking",
      triggeredByValue = "true"
  )
  public String knownHosts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      group = "REMOTE"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      label = "Poll Interval",
      description = "Time (in seconds) between polling the remote service for new files",
      group = "REMOTE"
  )
  public int pollInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Archive on error",
      description = "On error, should the file be archive to a local directory",
      group = "ERROR"
  )
  public boolean archiveOnError;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Archive Directory",
      description = "Directory to archive files, if an irrecoverable error is encountered",
      group = "ERROR",
      dependsOn = "archiveOnError",
      triggeredByValue = "true"
  )
  public String errorArchiveDir = "";

}
