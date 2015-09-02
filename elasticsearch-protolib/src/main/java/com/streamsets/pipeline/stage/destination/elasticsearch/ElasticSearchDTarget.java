/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Elasticsearch",
    description = "Upload data to an Elasticsearch cluster",
    icon = "elasticsearch.png"
)
@ConfigGroups(Groups.class)
public class ElasticSearchDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "elasticsearch",
      label = "Cluster Name",
      description = "",
      displayPosition = 10,
      group = "ELASTIC_SEARCH"
  )
  public String clusterName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Cluster URIs",
      defaultValue = "[\"localhost:9300\"]",
      description = "Elasticsearch Node URIs",
      displayPosition = 20,
      group = "ELASTIC_SEARCH"
  )
  public List<String> uris;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "{ \"client.transport.sniff\" : \"true\" }",
      label = "Additional Configuration",
      description = "Additional Elasticsearch client configuration properties",
      displayPosition = 30,
      group = "ELASTIC_SEARCH"
  )
  public Map<String, String> configs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:value('/es-index')}",
      label = "Index",
      description = "",
      displayPosition = 40,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String indexTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:value('/es-mapping')}",
      label = "Mapping",
      description = "",
      displayPosition = 50,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String typeTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Document ID",
      description = "Typically left empty",
      displayPosition = 50,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String docIdTemplate;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "UTF-8",
    label = "Data Charset",
    displayPosition = 55,
    group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @Override
  protected Target createTarget() {
    return new ElasticSearchTarget(clusterName, uris, configs, indexTemplate, typeTemplate, docIdTemplate, charset);
  }

}
