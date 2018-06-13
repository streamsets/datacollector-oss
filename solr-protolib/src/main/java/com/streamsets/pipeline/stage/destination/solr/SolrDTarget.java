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
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 3,
    label = "Solr",
    description = "Upload data to an Apache Solr",
    icon = "solr.png",
    onlineHelpRefUrl ="index.html?contextID=task_ld1_phr_wr",
    upgrader = SolrDTargetUpgrader.class
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
  @ValueChooserModel(InstanceTypeOptionsChooserValues.class)
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
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Default Collection Name",
      description = "",
      displayPosition = 30,
      group = "SOLR",
      dependsOn = "instanceType",
      triggeredByValue = { "SOLR_CLOUD"}
  )
  public String defaultCollection;

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
  @ValueChooserModel(ProcessingModeChooserValues.class)
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
  @ListBeanModel
  public List<SolrFieldMappingConfig> fieldNamesMap;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Ignore Optional Fields",
      displayPosition = 59,
      group = "SOLR"
  )
  public boolean ignoreOptionalFields = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "TO_ERROR",
      label = "Missing Fields",
      description = "Action to take when fields are empty",
      displayPosition = 60,
      group = "SOLR"
  )
  @ValueChooserModel(MissingFieldActionChooserValues.class)
  public MissingFieldAction missingFieldAction = MissingFieldAction.TO_ERROR;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Kerberos Authentication",
      displayPosition = 70,
      group = "SOLR"
  )
  public boolean kerberosAuth;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Skip Validation",
      displayPosition = 80,
      group = "SOLR"
  )
  public boolean skipValidation;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Wait Flush",
      description = "Block until index changes are flushed to disk",
      displayPosition = 1000,
      group = "SOLR"
  )
  public boolean waitFlush = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Wait Searcher",
      description = "Block until a new searcher is opened and registered as the main query searcher, making the" +
          " changes visible",
      displayPosition = 1010,
      group = "SOLR"
  )
  public boolean waitSearcher = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Soft Commit",
      description = "Makes index changes visible while neither fsync-ing index files nor writing a new index" +
          " descriptor",
      displayPosition = 1020,
      group = "SOLR"
  )
  public boolean softCommit = false;

  @Override
  protected Target createTarget() {
    return new SolrTarget(
        instanceType,
        solrURI,
        zookeeperConnect,
        indexingMode,
        fieldNamesMap,
        defaultCollection,
        kerberosAuth,
        missingFieldAction,
        skipValidation,
        waitFlush,
        waitSearcher,
        softCommit,
        ignoreOptionalFields
    );
  }

}
