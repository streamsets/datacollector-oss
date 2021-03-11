/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.connection.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.ValueChooserModel;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
  label = "SFTP/FTP/FTPS",
  type = RemoteConnection.TYPE,
  description = "SFTP/FTP/FTPS connector",
  version = 1,
  upgraderDef = "upgrader/RemoteConnection.yaml",
  supportedEngines = {ConnectionEngine.COLLECTOR}
)
@ConfigGroups(RemoteConnectionGroups.class)
public class RemoteConnection {

  public static final String TYPE = "STREAMSETS_REMOTE_FILE";

  @ConfigDefBean(groups = "#1")
  public CredentialsConfig credentials = new CredentialsConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "sftp://host:port",
      label = "Resource URL",
      description = "Specify the SFTP/FTP/FTPS URL",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String remoteAddress;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SFTP",
      label = "Protocol",
      description = "Protocol to connect to the server.",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(ProtocolChooserValues.class)
  public Protocol protocol = Protocol.SFTP;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "EXPLICIT",
      label = "FTPS Mode",
      description = "FTP encryption negotiation mode. \"Implicit\" mode assumes that encryption will be used " +
          "immediately. \"Explicit\" mode (also called FTPES) means that plain FTP will be used to connect and " +
          "then encryption will be negotiated.",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = "protocol",
      triggeredByValue = "FTPS"
  )
  @ValueChooserModel(FTPSModeChooserValues.class)
  public FTPSMode ftpsMode = FTPSMode.EXPLICIT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "PRIVATE",
      label = "FTPS Data Channel Protection Level",
      description = "Sets the FTPS data channel protection level to either \"Clear\" (equivalent of \"PROT C\") or " +
          "\"Private\" (equivalent of \"PROT P\").  \"Private\" means that the communication and data are both " +
          "encrypted, while \"Clear\" means that only the communication is encrypted.",
      displayPosition = 65,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = "protocol",
      triggeredByValue = "FTPS"
  )
  @ValueChooserModel(FTPSDataChannelProtectionLevelChooserValues.class)
  public FTPSDataChannelProtectionLevel ftpsDataChannelProtectionLevel = FTPSDataChannelProtectionLevel.PRIVATE;

}
