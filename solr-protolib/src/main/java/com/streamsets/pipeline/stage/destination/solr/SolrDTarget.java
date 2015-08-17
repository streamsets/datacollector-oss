/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Solr",
    description = "Upload data to an Apache Solr",
    icon = "solr.png"
)
@ConfigGroups(Groups.class)
public class SolrDTarget extends DTarget {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SINGLE_NODE",
    label = "Instance Type",
    description = "",
    displayPosition = 10,
    group = "SOLR"
  )
  @ValueChooser(InstanceTypeOptionsChooserValues.class)
  public InstanceTypeOptions instanceType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "http://localhost:8983/solr/corename",
    label = "Solr URI",
    description = "",
    displayPosition = 20,
    group = "SOLR",
    dependsOn = "instanceType",
    triggeredByValue = { "SINGLE_NODE"}
  )
  public String solrURI;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:9983",
    label = "ZooKeeper Connection String",
    description = "Comma-separated list of the Zookeeper <HOST>:<PORT> used by the SolrCloud",
    displayPosition = 30,
    group = "SOLR",
    dependsOn = "instanceType",
    triggeredByValue = { "SOLR_CLOUD"}
  )
  public String zookeeperConnect;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "BATCH",
    label = "Record Indexing Mode",
    description = "If 'Record by Record' the destination indexes one record at a time, if 'Record batch' " +
      "the destination bulk indexes all the records in the batch. ",
    displayPosition = 40,
    group = "SOLR"
  )
  @ValueChooser(ProcessingModeChooserValues.class)
  public ProcessingMode indexingMode;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="",
    label = "Fields",
    description = "Selected fields are mapped to columns of the same name. These should match your schema",
    displayPosition = 50,
    group = "SOLR"
  )
  @ComplexField
  public List<SolrFieldMappingConfig> fieldNamesMap;

  @Override
  protected Target createTarget() {
    return new SolrTarget(instanceType, solrURI, zookeeperConnect, indexingMode, fieldNamesMap);
  }

}
