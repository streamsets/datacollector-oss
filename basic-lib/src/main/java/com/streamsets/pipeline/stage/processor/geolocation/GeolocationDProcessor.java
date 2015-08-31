/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=1,
    label="Geo IP",
    description = "IP address geolocation using a Maxmind GeoIP2 database file",
    icon="globe.png"
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
    defaultValue="",
    label = "",
    description = "",
    displayPosition = 10,
    group = "GEOLOCATION"
  )
  @ListBeanModel
  public List<GeolocationFieldConfig> fieldTypeConverterConfigs;

  @Override
  protected Processor createProcessor() {
    return new GeolocationProcessor(geoIP2DBFile,
      fieldTypeConverterConfigs);
  }
}
