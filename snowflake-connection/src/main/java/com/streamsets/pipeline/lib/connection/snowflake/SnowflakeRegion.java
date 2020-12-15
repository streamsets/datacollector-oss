/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.connection.snowflake;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum SnowflakeRegion implements Label {
  US_WEST_2("AWS - US West (default)"),
  US_EAST_1("AWS - US East (us-east-1)"),
  CA_CENTRAL_1("AWS - Canada Central (ca-central-1)"),
  EU_WEST_1("AWS - EU (Ireland) (eu-west-1)"),
  EU_CENTRAL_1("AWS - EU (Frankfurt) (eu-central-1)"),
  AP_SOUTHEAST_1("AWS - Asia Pacific (Singapore) (ap-southeast-1"),
  AP_SOUTHEAST_2("AWS - Asia Pacific (Sydney) (ap-southeast-2)"),
  EAST_US_2__AZURE("Microsoft Azure East US 2 (east-us-2.azure)"),
  CANADA_CENTRAL__AZURE("Microsoft Azure Canada Central (canada-central.azure"),
  WEST_EUROPE__AZURE("Microsoft Azure West Europe (west-europe.azure)"),
  AUSTRALIA_EAST__AZURE("Microsoft Azure Australia East (australia-east.azure)"),
  SOUTHEAST_ASIA__AZURE("Microsoft Azure Southeast Asia (southeast-asia.azure)"),
  OTHER("Other - specify"),
  CUSTOM_URL("Custom JDBC URL - Virtual Private Snowflake")
  ;

  private final String label;

  SnowflakeRegion(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public String getId() {
    // double __ gets converted to .
    // single _ gets coverted to -
    return name().toLowerCase().replace("__", ".").replace('_', '-');
  }

}
