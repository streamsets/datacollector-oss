/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=2,
    label="Geo IP",
    description = "IP address geolocation using a Maxmind GeoIP2 database file",
    icon="globe.png",
    onlineHelpRefUrl = "index.html#Processors/GeoIP.html#task_wpz_nhs_ns",
    upgrader = GeolocationProcessorUpgrader.class
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class GeolocationDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "GeoIP2 Database File",
      description = "An absolute path or a file under SDC resources directory in GeoIP2 format",
      displayPosition = 10,
      group = "GEOLOCATION"
  )
  public String geoIP2DBFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "GeoIP2 Database Type",
      description = "The type of GeoIP2 database being used",
      displayPosition = 11,
      group = "GEOLOCATION"
  )
  @ValueChooserModel(GeolocationDBTypeEnumChooserValues.class)
  public GeolocationDBType geoIP2DBType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="",
    label = "",
    description = "",
    displayPosition = 20,
    group = "GEOLOCATION"
  )
  @ListBeanModel
  public List<GeolocationFieldConfig> fieldTypeConverterConfigs;

  @Override
  protected Processor createProcessor() {
    return new GeolocationProcessor(geoIP2DBFile,
      geoIP2DBType,
      fieldTypeConverterConfigs);
  }
}
