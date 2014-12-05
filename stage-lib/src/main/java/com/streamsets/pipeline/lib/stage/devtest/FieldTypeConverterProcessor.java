/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import java.util.Map;

@GenerateResourceBundle
@StageDef(version="1.0.0", label="Field Type Converter")
public class FieldTypeConverterProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(label = "Fields to convert", required = false,type = Type.MODEL, defaultValue="")
  @FieldValueChooser(type= ChooserMode.PROVIDED, chooserValues = ConverterValuesProvider.class)
  public Map<String, FieldType> fields;

  // the annotations processor will fail if variable is not Map

  /* bundle
   stage.label=
   stage.description=
   config.#NAME#.label=
   config.#NAME#.description=
   config.#NAME#.value.BOOLEAN=
   config.#NAME#.value.BYTE=
   config.#NAME#.value.CHAR=
   config.#NAME#.value.INTEGER=
   config.#NAME#.value.LONG=
   */

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

  }
}
