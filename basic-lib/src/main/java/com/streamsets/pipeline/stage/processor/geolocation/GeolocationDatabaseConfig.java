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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class GeolocationDatabaseConfig {

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
      displayPosition = 10,
      group = "GEOLOCATION"
  )
  @ValueChooserModel(GeolocationDBTypeEnumChooserValues.class)
  public GeolocationDBType geoIP2DBType;

  @Override
  public String toString() {
    return "GeolocationFieldConfig{" +
      "geoIP2DBFile='" + geoIP2DBFile+ '\'' +
      ", geoIP2DBType='" + geoIP2DBType + '\'' +
      '}';
  }
}
