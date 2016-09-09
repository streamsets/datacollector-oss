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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSourceOffsetCommitter;

@StageDef(
    version = 4,
    label = "Kinesis Consumer",
    description = "Reads data from Kinesis",
    icon = "kinesis.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = false,
    upgrader = KinesisSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/KinConsumer.html#task_p4b_vv4_yr"
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class KinesisDSource extends DSourceOffsetCommitter {

  @ConfigDefBean(groups = {"KINESIS", "ADVANCED"})
  public KinesisConsumerConfigBean kinesisConfig;

  @Override
  protected Source createSource() {
    return new KinesisSource(kinesisConfig);
  }
}
