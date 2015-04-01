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
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Elastic Search",
    description = "Upload data to an Elastic Search cluster",
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
      label = "Cluster URI(s)",
      defaultValue = "[\"localhost:9300\"]",
      description = "Elastic Search Node URIs",
      displayPosition = 20,
      group = "ELASTIC_SEARCH"
  )
  public List<String> uris;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "[{\"key\": \"client.transport.sniff\", \"value\" : \"true\"}]",
      label = "Additional Configuration",
      description = "Additional Elastic Search client configuration properties",
      displayPosition = 30,
      group = "ELASTIC_SEARCH"
  )
  public Map<String, String> configs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "${record:value('/es-index')}",
      label = "Index",
      description = "",
      displayPosition = 40,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class}
  )
  public String indexTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "${record:value('/es-type')}",
      label = "Type",
      description = "",
      displayPosition = 50,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class}
  )
  public String typeTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.EL_STRING,
      defaultValue = "${record:value('/es-id')}",
      label = "Document ID",
      description = "Typically left empty",
      displayPosition = 50,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class}
  )
  public String docIdTemplate;

  @Override
  protected Target createTarget() {
    return new ElasticSearchTarget(clusterName, uris, configs, indexTemplate, typeTemplate, docIdTemplate);
  }

}
