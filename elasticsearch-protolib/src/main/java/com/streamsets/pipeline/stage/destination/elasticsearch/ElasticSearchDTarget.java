/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
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
  @ValueChooser(CharsetChooserValues.class)
  public String charset;

  @Override
  protected Target createTarget() {
    return new ElasticSearchTarget(clusterName, uris, configs, indexTemplate, typeTemplate, docIdTemplate, charset);
  }

}
