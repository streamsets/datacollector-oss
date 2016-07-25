/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.config.FileRawSourcePreviewer;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 3,
    label = "File Tail",
    description = "Tails a file. It handles rolling files within the same directory",
    icon = "fileTail.png",
    execution = ExecutionMode.STANDALONE,
    outputStreams = FileTailOutputStreams.class,
    recordsByRef = true,
    upgrader = FileTailSourceUpgrader.class,
    resetOffset = true,
    producesEvents = true,
    onlineHelpRefUrl = "index.html#Origins/FileTail.html#task_unq_wdw_yq"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FileTailDSource extends DSource {

  @ConfigDefBean
  public FileTailConfigBean conf;

  @Override
  protected Source createSource() {
    return new FileTailSource(conf);
  }

}
