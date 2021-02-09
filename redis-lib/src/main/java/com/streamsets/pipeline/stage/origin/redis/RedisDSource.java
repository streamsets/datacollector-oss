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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

@StageDef(
    version = 5,
    label = "Redis Consumer",
    description = "Reads data from Redis",
    icon = "redis.png",
    upgrader = RedisSourceUpgrader.class,
    upgraderDef = "upgrader/RedisDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_dtz_npv_jw",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true

)
@ConfigGroups(value = Groups.class)
@HideConfigs(value = {"redisOriginConfigBean.dataFormatConfig.compression"})
@GenerateResourceBundle
public class RedisDSource extends DSource {

  @ConfigDefBean()
  public RedisOriginConfigBean redisOriginConfigBean;

  /**
   * {@inheritDoc}
   */
  @Override
  protected Source createSource() {
    return new RedisSubscriptionSource(redisOriginConfigBean);
  }
}
