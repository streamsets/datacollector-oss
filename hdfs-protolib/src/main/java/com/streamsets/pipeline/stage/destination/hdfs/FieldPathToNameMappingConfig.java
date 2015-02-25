/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;

import java.util.List;

public class FieldPathToNameMappingConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Field Path",
      description = "The fields which must be written to the target",
      displayPosition = 10
  )
  @FieldSelector
  public List<String> fields;

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Delimited Column Name",
      description = "The name which must be used for the fields in the target",
      displayPosition = 20
  )
  public String name;
}
