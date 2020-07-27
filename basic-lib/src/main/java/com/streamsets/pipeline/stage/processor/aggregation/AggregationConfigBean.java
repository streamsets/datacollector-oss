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
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class AggregationConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ROLLING",
      label = "Window Type",
      description = "",
      group = "AGGREGATIONS",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {TimeConstantsEL.class}
  )
  @ValueChooserModel(WindowTypeChooserValues.class)
  public WindowType windowType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TW_5S",
      label = "Time Window",
      description = "",
      group = "AGGREGATIONS",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {TimeConstantsEL.class},
      dependsOn = "windowType",
      triggeredByValue = "ROLLING"
  )
  @ValueChooserModel(TimeWindowChooserValues.class)
  public TimeWindow timeWindow;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TW_5S",
      label = "Time Window",
      description = "",
      group = "AGGREGATIONS",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {TimeConstantsEL.class},
      dependsOn = "windowType",
      triggeredByValue = "SLIDING"
  )
  @ValueChooserModel(WindowLengthChooserValues.class)
  public TimeWindow windowLength;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Time Zone",
      description = "Time zone to use for time windows",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "AGGREGATIONS"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "3",
      label = "Number of Time Windows to Remember",
      description = "",
      group = "AGGREGATIONS",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      max = 10,
      dependsOn = "windowType",
      triggeredByValue = "ROLLING"
  )
  public int timeWindowsToRemember;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Aggregations",
      defaultValue = "",
      description = "",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "AGGREGATIONS"
  )
  @ListBeanModel
  public List<AggregatorConfig> aggregatorConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "All Aggregators Event",
      description = "Produce one event record for all aggregations computed by this processor",
      group = "EVENTS",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean allAggregatorsEvent;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Per Aggregator Events",
      description = "Produce one record for each aggregation computed by this processor",
      group = "EVENTS",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean perAggregatorEvents;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Produce Event Record with Text Field",
      description = "When this option is chosen the aggregation data is written in a String field as a JSON string in the event record",
      group = "EVENTS",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean eventRecordWithTextField;

  private TimeZone timeZone;

  protected List<Stage.ConfigIssue> init(Processor.Context context) {
    List<Stage.ConfigIssue> configIssues = new ArrayList<>();

    this.timeZone = TimeZone.getTimeZone(timeZoneID);

    // validate unique Aggregation names
    List<String> aggregateConfigNames = new ArrayList<>();
    aggregatorConfigs.forEach(aggregatorConfig -> {
      aggregateConfigNames.add(aggregatorConfig.aggregationName);
    });
    List<String> duplicates = aggregateConfigNames.stream().distinct().filter(
        entry -> Collections.frequency(aggregateConfigNames, entry) > 1).collect(
            Collectors.toList()
    );
    if (duplicates.size() > 0) {
      configIssues.add(context.createConfigIssue(
          Groups.AGGREGATIONS.name(),
          "config.aggregatorConfigs",
          Errors.AGGREGATOR_00,
          String.join(", ", duplicates)
      ));
    }

    // TODO validate all configs

    return configIssues;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }


  public int getNumberOfTimeWindows() {
    return (windowType == WindowType.ROLLING) ? timeWindowsToRemember : windowLength.getNumberOfMicroTimeWindows();
  }

  public TimeWindow getRollingTimeWindow() {
    return (windowType == WindowType.ROLLING) ? timeWindow : windowLength.getMicroTimeWindow();
  }

  public String getTimeWindowLabel() {
    return (windowType == WindowType.ROLLING) ? timeWindow.getLabel() : windowLength.getLabel();
  }

}
