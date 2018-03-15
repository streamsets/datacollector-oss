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
package com.streamsets.pipeline.stage.processor.jdbcmetadata;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;

public class DecimalDefaultsConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = HeaderAttributeConstants.ATTR_SCALE,
      label = "Decimal Scale Attribute",
      description = "Name of the field attribute that stores precision for decimal fields.",
      displayPosition = 80,
      group = "JDBC"
  )
  public String scaleAttribute;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = HeaderAttributeConstants.ATTR_PRECISION,
      label = "Decimal Precision Attribute",
      description = "Name of the field attribute that stores scale for decimal fields.",
      displayPosition = 90,
      group = "JDBC"
  )
  public String precisionAttribute;
}
