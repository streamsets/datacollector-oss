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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=4,
    label="Geo IP",
    description = "IP address geolocation using a Maxmind GeoIP2 database file",
    icon="globe.png",
    onlineHelpRefUrl ="index.html?contextID=task_wpz_nhs_ns",
    upgrader = GeolocationProcessorUpgrader.class,
    upgraderDef = "upgrader/GeolocationDProcessor.yaml"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class GeolocationDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "GeoIP2 Databases",
      description = "MaxMind GeoIP2 database file paths and database types.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GEOLOCATION"
  )
  @ListBeanModel
  public List<GeolocationDatabaseConfig> dbConfigs;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="",
    label = "Database Field Mappings",
    description = "Mappings of database fields to record fields.",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "GEOLOCATION"
  )
  @ListBeanModel
  public List<GeolocationFieldConfig> fieldTypeConverterConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "Missing Address Action",
      description = "Action to perform on record if IP address is missing from database",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GEOLOCATION"
  )
  @ValueChooserModel(GeolocationMissingAddressEnumChooserValues.class)
  public GeolocationMissingAddressAction missingAddressAction;

  @Override
  protected Processor createProcessor() {
    return new GeolocationProcessor(dbConfigs, missingAddressAction, fieldTypeConverterConfigs);
  }
}
