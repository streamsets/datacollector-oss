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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection;
import com.streamsets.pipeline.stage.destination.elasticsearch.ElasticsearchOperationChooserValues;
import com.streamsets.pipeline.stage.destination.elasticsearch.ElasticsearchOperationType;
import com.streamsets.pipeline.stage.destination.elasticsearch.UnsupportedOperationAction;
import com.streamsets.pipeline.stage.destination.elasticsearch.UnsupportedOperationActionChooserValues;

public class ElasticsearchTargetConfig extends ElasticsearchConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<field path>\")}'.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve the datetime of a time based index",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Index",
      description = "",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String indexTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Mapping",
      description = "",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String typeTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Document ID",
      description = "An expression which evaluates to a document ID. Required for create/update/delete. " +
          "Optional for index, leave blank to use auto-generated IDs.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String docIdTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Parent ID",
      description = "An expression which evaluates to a document ID for the parent in a parent/child hierarchy.",
      displayPosition = 85,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String parentIdTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Routing",
      description = "An expression which evaluates to a document ID whose shard will be indexed on.",
      displayPosition = 87,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String routingTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Data Charset",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "INDEX",
      label = "Default Operation",
      description = "Default operation to perform if sdc.operation.type is not set in record header.",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(ElasticsearchOperationChooserValues.class)
  public ElasticsearchOperationType defaultOperation = ElasticsearchOperationType.INDEX;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue= "DISCARD",
      label = "Unsupported Operation Handling",
      description = "Action to take when operation type is not supported",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(UnsupportedOperationActionChooserValues.class)
  public UnsupportedOperationAction unsupportedAction = UnsupportedOperationAction.DISCARD;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.JSON,
      elDefs = {RecordEL.class},
      label = "Additional Properties",
      description = "Additional properties for the request",
      defaultValue = "{\n}",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ELASTIC_SEARCH"
  )
  public String rawAdditionalProperties;
}
