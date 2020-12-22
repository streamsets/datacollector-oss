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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchTargetConfig;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;

@GenerateResourceBundle
@StageDef(
    // We're reusing upgrader for both ToErrorElasticSearchDTarget and ElasticsearchDTargetUpgrader, make sure that you
    // upgrade both versions at the same time when changing.
    version = 12,
    label = "Elasticsearch",
    description = "Upload data to an Elasticsearch cluster",
    icon = "elasticsearch.png",
    onlineHelpRefUrl ="index.html?contextID=task_uns_gtv_4r",
    upgrader = ElasticsearchDTargetUpgrader.class,
    upgraderDef = "upgrader/ElasticSearchDTarget.yaml"
)
@ConfigGroups(ElasticsearchConnectionGroups.class)
public class ElasticSearchDTarget extends DTarget {

  @ConfigDefBean
  public ElasticsearchTargetConfig elasticSearchConfig;

  @Override
  protected Target createTarget() {
    return new ElasticsearchTarget(elasticSearchConfig);
  }

}
