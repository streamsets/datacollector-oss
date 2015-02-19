/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.List;

public class FieldHasherConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL, defaultValue="",
      label = "Fields to Hash",
      description = "Hash string fields. You can enter multiple fields for the same hash type.",
      displayPosition = 10
  )
  @FieldSelector
  public List<String> fieldsToHash;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="MD5",
      label = "Hash Type",
      description="",
      displayPosition = 20
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = HashTypeChooserValues.class)
  public HashType hashType;
}
