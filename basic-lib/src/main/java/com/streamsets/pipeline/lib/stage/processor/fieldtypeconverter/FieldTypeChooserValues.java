package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class FieldTypeChooserValues extends BaseEnumChooserValues<Field.Type> {

  public FieldTypeChooserValues() {
    super(Field.Type.BOOLEAN, Field.Type.CHAR, Field.Type.BYTE, Field.Type.SHORT, Field.Type.INTEGER, Field.Type.LONG,
          Field.Type.FLOAT, Field.Type.DOUBLE, Field.Type.DECIMAL, Field.Type.DATE, Field.Type.DATETIME,
          Field.Type.STRING, Field.Type.BYTE_ARRAY);
  }

}