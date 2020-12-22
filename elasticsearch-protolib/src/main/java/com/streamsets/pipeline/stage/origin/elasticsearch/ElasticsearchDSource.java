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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;

@GenerateResourceBundle
@StageDef(
    version = 3,
    label = "Elasticsearch",
    description = "Read data from an Elasticsearch cluster",
    icon = "elasticsearch_multithreaded.png",
    recordsByRef = true,
    resetOffset = true,
    upgraderDef = "upgrader/ElasticsearchDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_pmh_xpm_2z"
)
@ConfigGroups(ElasticsearchConnectionGroups.class)
public class ElasticsearchDSource extends DPushSource {

  @ConfigDefBean
  public ElasticsearchSourceConfig conf;

  @Override
  protected PushSource createPushSource() {
    return new ElasticsearchSource(conf);
  }
}
