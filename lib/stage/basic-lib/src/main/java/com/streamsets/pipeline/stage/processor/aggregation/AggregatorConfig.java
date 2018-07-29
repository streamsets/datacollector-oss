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
package com.streamsets.pipeline.stage.processor.aggregation;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class AggregatorConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enabled",
      description = "Enable the aggregation configuration",
      displayPosition = 0,
      group = "AGGREGATIONS"
  )
  public boolean enabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Name",
      description = "When there are multiple aggregations, the names identify them uniquely",
      displayPosition = 5,
      group = "AGGREGATIONS"
  )
  public String aggregationName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Aggregation Title",
      description = "Enter the title you want to see in the metrics view for this aggregation",
      displayPosition = 10,
      group = "AGGREGATIONS"
  )
  public String aggregationTitle;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Filter",
      description = "Select to enable filtering records for aggregation",
      displayPosition = 20,
      group = "AGGREGATIONS"
  )
  public boolean filter;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Filter Predicate",
      description = "An expression to filter records for aggregation",
      displayPosition = 30,
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "filter",
      triggeredByValue = "true",
      group = "AGGREGATIONS"
  )
  public String filterPredicate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Aggregation Function",
      description = " ",
      defaultValue = "COUNT",
      displayPosition = 40,
      group = "AGGREGATIONS"
  )
  @ValueChooserModel(AggregationFunctionsChooserValues.class)
  public AggregationFunction aggregationFunction;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "1",
      label = "Aggregation Expression",
      description = "The resulting value of this expression will be aggregated",
      displayPosition = 50,
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "AGGREGATIONS",
      dependsOn = "aggregationFunction",
      triggeredByValue = {"AVG_DOUBLE", "AVG_INTEGER", "STD_DEV", "MIN_DOUBLE", "MIN_INTEGER", "MAX_DOUBLE", "MAX_INTEGER", "SUM_DOUBLE", "SUM_INTEGER"}
  )
  public String aggregationExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Group By",
      description = "",
      displayPosition = 60,
      group = "AGGREGATIONS"
  )
  public boolean groupBy;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Group By Expression",
      description = "Aggregations will be grouped by the resulting values of this expression",
      displayPosition = 70,
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "groupBy",
      triggeredByValue = "true",
      group = "AGGREGATIONS"
  )
  public String groupByExpression;



}
