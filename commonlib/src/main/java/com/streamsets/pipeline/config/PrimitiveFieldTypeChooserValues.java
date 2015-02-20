package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class PrimitiveFieldTypeChooserValues extends BaseEnumChooserValues<Field.Type> {

  public PrimitiveFieldTypeChooserValues() {
    super(Field.Type.BOOLEAN, Field.Type.CHAR, Field.Type.BYTE, Field.Type.SHORT, Field.Type.INTEGER, Field.Type.LONG,
          Field.Type.FLOAT, Field.Type.DOUBLE, Field.Type.DECIMAL, Field.Type.DATE, Field.Type.DATETIME,
          Field.Type.STRING, Field.Type.BYTE_ARRAY);
  }

}