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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.credential.CredentialValue;
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
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Authentication",
      description = "The authentication method to use to login to remote server",
      displayPosition = 10,
      group = "CREDENTIALS"
  )
  @ValueChooserModel(AuthenticationChooserValues.class)
  public Authentication auth = Authentication.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Username to use to login to the remote server",
      displayPosition = 15,
      group = "CREDENTIALS",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD", "PRIVATE_KEY"}
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password to use to login to the remote server. If private key is specified, that is used.",
      displayPosition = 20,
      group = "CREDENTIALS",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD"}
  )
  public CredentialValue password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Private Key File",
      description = "Private Key File to use to login to the remote server.",
      displayPosition = 30,
      group = "CREDENTIALS",
      dependsOn = "auth",
      triggeredByValue = {"PRIVATE_KEY"}
  )
  public String privateKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Private Key Passphrase",
      description = "Passphrase to use to open the private key file.",
      displayPosition = 40,
      group = "CREDENTIALS",
      dependsOn = "auth",
      triggeredByValue = {"PRIVATE_KEY"}
  )
  public CredentialValue privateKeyPassphrase;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Path Relative to User Home Directory",
      description = "If checked, the path is resolved relative to the logged in user's home directory, " +
          "if a username is entered in the Credentials tab or in the URL.",
      displayPosition = 20,
      group = "REMOTE"
  )
  public boolean userDirIsRoot = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Strict Host Checking",
      description = "If enabled, this client will only connect to the host if the host is in the known hosts file.",
      displayPosition = 50,
      group = "CREDENTIALS"
  )
  public boolean strictHostChecking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Known Hosts file",
      description = "Full path to the file that lists the host keys of all known hosts." +
          "This must be specified if the strict host checking is enabled.",
      group = "CREDENTIALS",
      displayPosition = 60,
      dependsOn = "strictHostChecking",
      triggeredByValue = "true"
  )
  public String knownHosts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      group = "DATA_FORMAT",
      displayPosition = 1
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Archive on error",
      description = "On error, should the file be archive to a local directory",
      group = "ERROR",
      displayPosition = 10
  )
  public boolean archiveOnError;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Archive Directory",
      description = "Directory to archive files, if an irrecoverable error is encountered",
      group = "ERROR",
      displayPosition = 20,
      dependsOn = "archiveOnError",
      triggeredByValue = "true"
  )
  public String errorArchiveDir = "";


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Process Subdirectories",
      description = "Process files in subdirectories of the specified path",
      group = "REMOTE",
      displayPosition = 30
  )
  public boolean processSubDirectories;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "File Name Pattern",
      defaultValue = "*",
      description =  "A glob that defines the pattern of the file names in the directory. ('*' selects all files)" +
          "Files are processed in chronological order.",
      group = "REMOTE",
      displayPosition = 40
  )
  public String filePattern;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "First File to Process",
      description = "When configured, the Data Collector does not process earlier file names",
      displayPosition = 50,
      group = "REMOTE"
  )
  public String initialFileToProcess;

}
