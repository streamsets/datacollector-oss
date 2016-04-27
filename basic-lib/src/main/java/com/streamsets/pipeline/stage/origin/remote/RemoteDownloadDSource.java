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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 3,
    label = "SFTP Client",
    description = "Uses an SFTP client to read records from an URL.",
    icon = "httpclient.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = "index.html#Origins/SFTP.html#task_lfx_fzd_5v"
)
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class RemoteDownloadDSource extends DSource{

  @ConfigDefBean
  public RemoteDownloadConfigBean conf;

  @Override
  protected Source createSource() {
    return new RemoteDownloadSource(
        conf.remoteAddress,
        conf.username,
        conf.password,
        conf.knownHosts,
        !conf.strictHostChecking,
        conf.dataFormatConfig,
        conf.dataFormat,
        conf.pollInterval,
        conf.errorArchiveDir
    );
  }
}
