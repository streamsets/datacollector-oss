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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "MapR DB JSON Origin",
    description = "Retrieves Documents from MapR DB JSON Document Database",
    icon = "mapr_db.png",
    resetOffset = true,
    privateClassLoader = true,
    onlineHelpRefUrl ="index.html?contextID=task_hys_s15_3y",
    upgrader = MaprJsonSourceUpgrader.class
)

@ConfigGroups(Groups.class)
public class MapRJsonOriginDSource extends DSource {
  public static final String MAPR_JSON_ORIGIN_CONFIG_BEAN_PREFIX = "mapRJsonOriginConfigBean";

  @ConfigDefBean(groups="MAPR_JSON_ORIGIN")
  public MapRJsonOriginConfigBean mapRJsonOriginConfigBean;

  @Override
  protected Source createSource() {
    return new MapRJsonOriginSource(mapRJsonOriginConfigBean);
  }

}
